//! The state tables' local-filesystem opendal backend. Everything delegates to the stock `Fs`
//! service except `write`: the stock service pays `create_dir_all(parent)` — a `mkdir` plus its
//! `stat` — on every file it writes, which the backend CPU profile showed as the single largest
//! cost (state tables write one file per touched bucket per commit, plus manifests and snapshot
//! documents). A state table's directory skeleton is created once and lives for the table's whole
//! life, so writes here open the file directly and create the parent only on the rare miss (one
//! retry), making the steady state pay zero directory syscalls per file.

use datafusion::error::DataFusionError;
use opendal::raw::{
    oio, OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceDyn, ServiceInfo, Servicer,
};
use opendal::{Buffer, Capability, EntryMode, Metadata, OperationContext, Operator};
use opendal_service_fs::FsConfig;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

/// Builds the state tables' operator: the stock `Fs` service at root `/` (mirroring paimon-rust's
/// own local-fs backend, so paths resolve identically) wrapped with the direct write path. The
/// stock operator's operation context is reused, so delegated operations run exactly as they
/// would against the unwrapped service.
pub(crate) fn state_fs_operator() -> Result<Operator, DataFusionError> {
    let mut cfg = FsConfig::default();
    cfg.root = Some("/".to_string());
    let (ctx, inner) = Operator::from_config(cfg)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .into_parts();
    Ok(Operator::from_parts(
        ctx,
        std::sync::Arc::new(StateFsService { inner }),
    ))
}

#[derive(Debug)]
struct StateFsService {
    inner: Servicer,
}

impl Service for StateFsService {
    type Reader = oio::Reader;
    type Writer = StateFsWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info_dyn()
    }

    fn capability(&self) -> Capability {
        self.inner.capability_dyn()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir_dyn(ctx, path, args).await
    }

    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.inner.stat_dyn(ctx, path, args).await
    }

    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read_dyn(ctx, path, args)
    }

    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        if args.append() {
            return Ok(StateFsWriter::Inner(self.inner.write_dyn(ctx, path, args)?));
        }
        Ok(StateFsWriter::Direct {
            path: PathBuf::from("/").join(path),
            file: None,
            written: 0,
        })
    }

    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete_dyn(ctx)
    }

    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list_dyn(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy_dyn(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename_dyn(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.inner.presign_dyn(ctx, path, args).await
    }
}

/// The direct writer (overwrite-create semantics, matching the stock service). The file opens
/// lazily on the first write; a missing parent directory is the rare exception handled by one
/// `create_dir_all` + retry, never a per-file cost. Appends — which the state tables never use —
/// fall back to the stock writer untouched.
pub(crate) enum StateFsWriter {
    Direct {
        path: PathBuf,
        file: Option<tokio::fs::File>,
        written: u64,
    },
    Inner(oio::Writer),
}

impl StateFsWriter {
    async fn open(path: &PathBuf) -> opendal::Result<tokio::fs::File> {
        match tokio::fs::File::create(path).await {
            Ok(file) => Ok(file),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(from_io)?;
                }
                tokio::fs::File::create(path).await.map_err(from_io)
            }
            Err(e) => Err(from_io(e)),
        }
    }
}

impl oio::Write for StateFsWriter {
    async fn write(&mut self, bs: Buffer) -> opendal::Result<()> {
        match self {
            StateFsWriter::Direct { path, file, written } => {
                if file.is_none() {
                    *file = Some(Self::open(path).await?);
                }
                let file = file.as_mut().expect("state fs file opened");
                *written += bs.len() as u64;
                for chunk in bs {
                    file.write_all(&chunk).await.map_err(from_io)?;
                }
                Ok(())
            }
            StateFsWriter::Inner(writer) => writer.write_dyn(bs).await,
        }
    }

    async fn close(&mut self) -> opendal::Result<Metadata> {
        match self {
            StateFsWriter::Direct { path, file, written } => {
                if file.is_none() {
                    // A writer closed without writes must still create the (empty) file, like
                    // the stock service.
                    *file = Some(Self::open(path).await?);
                }
                let file = file.as_mut().expect("state fs file opened");
                file.flush().await.map_err(from_io)?;
                Ok(Metadata::new(EntryMode::FILE).with_content_length(*written))
            }
            StateFsWriter::Inner(writer) => writer.close_dyn().await,
        }
    }

    async fn abort(&mut self) -> opendal::Result<()> {
        match self {
            StateFsWriter::Direct { .. } => Ok(()),
            StateFsWriter::Inner(writer) => writer.abort_dyn().await,
        }
    }
}

fn from_io(e: std::io::Error) -> opendal::Error {
    opendal::Error::new(opendal::ErrorKind::Unexpected, "state table file write").set_source(e)
}
