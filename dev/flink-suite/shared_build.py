"""Transfer a fresh common suite build between runners without transferring test evidence."""

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import subprocess
import tempfile
import time


DOWNLOAD_DELAYS = (5, 15)
DOWNLOAD_TIMEOUT_SECONDS = 1800


def checksum_path(archive):
    return archive.with_name(archive.name + ".sha256")


def archive_digest(archive):
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_archive(archive):
    expected = checksum_path(archive).read_text().strip()
    if len(expected) != 64 or archive_digest(archive) != expected:
        raise ValueError(f"Shared build checksum mismatch: {archive}")


def pack(root, repository, version, archive):
    entries = prepare(root, repository, version)
    subprocess.run(
        ["tar", "--zstd", "-cf", str(archive.resolve()),
         "--exclude=surefire-reports", "--exclude=failsafe-reports",
         "-C", str(root), *entries], check=True
    )
    subprocess.run(["zstd", "--test", "--quiet", str(archive)], check=True)
    checksum_path(archive).write_text(archive_digest(archive) + "\n")


def download(archive, artifact, run_id, github_repository):
    archive = archive.resolve()
    archive.parent.mkdir(parents=True, exist_ok=True)
    for attempt in range(len(DOWNLOAD_DELAYS) + 1):
        print(f"Downloading shared build (attempt {attempt + 1}/{len(DOWNLOAD_DELAYS) + 1}, "
              f"timeout {DOWNLOAD_TIMEOUT_SECONDS} seconds)", flush=True)
        try:
            with tempfile.TemporaryDirectory(prefix="shared-build-", dir=archive.parent) as directory:
                subprocess.run(
                    ["gh", "run", "download", run_id, "--repo", github_repository,
                     "--name", artifact, "--dir", directory],
                    check=True, timeout=DOWNLOAD_TIMEOUT_SECONDS,
                    env={**os.environ, "TMPDIR": directory},
                )
                candidate = Path(directory) / archive.name
                verify_archive(candidate)
                candidate.replace(archive)
                checksum_path(candidate).replace(checksum_path(archive))
            return
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            if attempt == len(DOWNLOAD_DELAYS):
                raise
            print(f"Shared build download attempt {attempt + 1} failed: {error}; retrying", flush=True)
            time.sleep(DOWNLOAD_DELAYS[attempt])


def unpack(root, repository, version, archive):
    if root.exists() and any(root.iterdir()):
        raise ValueError("Restore needs an empty suite directory; it must not inherit old evidence")
    verify_archive(archive)
    root.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="shared-build-restore-", dir=root.parent) as directory:
        staged = Path(directory) / "suite"
        staged.mkdir()
        subprocess.run(
            ["tar", "--zstd", "-xf", str(archive.resolve()), "-C", str(staged)], check=True
        )
        restore(staged, repository, version, destination=root)
        if root.exists():
            root.rmdir()
        staged.replace(root)


def identity(repository, version):
    return {
        "revision": subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
        ).strip(),
        "flink_version": version,
        "platform": platform.system(),
        "architecture": platform.machine(),
    }


def prepare(root, repository, version):
    required = [
        root / f"flink-{version}/.git",
        root / "m2",
        root / "maven-wrapper",
        root / "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
        root / "streamfusion-classpath.txt",
        root / "runtime-shards.json",
    ]
    for path in required:
        if not path.exists():
            raise ValueError(f"Missing shared build input: {path}")
    require_maven(root)
    libraries = sorted((root / "streamfusion-source/native/target/debug").glob("libstreamfusion*"))
    libraries = [path for path in libraries if path.suffix in (".so", ".dylib")]
    if not libraries:
        raise ValueError("Missing freshly built StreamFusion native libraries")
    classpath = root / "streamfusion-classpath.txt"
    paths = [Path(entry).resolve() for entry in classpath.read_text().strip().split(":")]
    for path in paths:
        path.relative_to(root.resolve())
        if not path.is_file():
            raise ValueError(f"Missing classpath artifact: {path}")
    classpath.write_text(":".join(map(str, paths)))
    metadata = {**identity(repository, version), "suite_root": str(root.resolve())}
    (root / "shared-build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    # Keep executable bits and the clean pinned checkout. Cargo intermediates and copied
    # StreamFusion sources are unnecessary for test consumers and dominate archive size.
    entries = [f"flink-{version}", "m2", "maven-wrapper", "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar",
               "streamfusion-classpath.txt", "runtime-shards.json", "shared-build.json"]
    entries += [str(path.relative_to(root)) for path in libraries]
    entries += [path.name for path in sorted(root.glob("*-unshaded.jar"))]
    return entries


def restore(root, repository, version, destination=None):
    destination = root if destination is None else destination
    metadata = json.loads((root / "shared-build.json").read_text())
    for key, value in identity(repository, version).items():
        if metadata[key] != value:
            raise ValueError(f"Shared build {key} mismatch: {metadata[key]} != {value}")
    require_maven(root)
    old = Path(metadata["suite_root"])
    classpath = root / "streamfusion-classpath.txt"
    relocated = []
    for entry in classpath.read_text().strip().split(":"):
        try:
            path = root.resolve() / Path(entry).relative_to(old)
        except ValueError as error:
            raise ValueError(f"Classpath entry outside the shared build: {entry}") from error
        if not path.is_file():
            raise ValueError(f"Missing relocated classpath artifact: {path}")
        relocated.append(str(destination.resolve() / path.relative_to(root.resolve())))
    classpath.write_text(":".join(relocated))
    metadata["suite_root"] = str(destination.resolve())
    (root / "shared-build.json").write_text(json.dumps(metadata, indent=2) + "\n")


def require_maven(root):
    executables = (root / "maven-wrapper/wrapper/dists").glob("**/bin/mvn")
    if not any(path.is_file() and os.access(path, os.X_OK) for path in executables):
        raise ValueError("Missing prepared Maven wrapper distribution")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("pack", "restore", "download"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--version")
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--artifact")
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--github-repository", default=os.environ.get("GITHUB_REPOSITORY"))
    args = parser.parse_args()
    if args.command == "download":
        if not all((args.artifact, args.run_id, args.github_repository)):
            parser.error("download requires --artifact, a run ID and a GitHub repository")
    elif args.root is None or args.version is None:
        parser.error("pack and restore require --root and --version")
    try:
        if args.command == "pack":
            pack(args.root, args.repository, args.version, args.archive)
        elif args.command == "download":
            download(args.archive, args.artifact, args.run_id, args.github_repository)
        else:
            unpack(args.root, args.repository, args.version, args.archive)
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f"Shared suite build failed: {error}\n")


if __name__ == "__main__":
    main()
