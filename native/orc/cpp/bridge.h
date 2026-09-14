#pragma once
#include <nanoarrow/nanoarrow.h>
#include <stddef.h>
#include <stdint.h>
extern "C" {
typedef int (*SfRead)(void *, uint64_t, uint64_t, void *);
typedef int (*SfWrite)(void *, const void *, size_t);
const char *sf_orc_error();
void *sf_orc_writer_new(const ArrowSchema *, const char *, const char *const *,
                        const char *const *, size_t, SfWrite, void *);
int sf_orc_writer_write(void *, const ArrowArray *);
int sf_orc_writer_finish(void *);
uint64_t sf_orc_writer_size(void *);
void sf_orc_writer_free(void *);
void *sf_orc_reader_new(const ArrowSchema *, const char *const *, size_t,
                        uint64_t, uint64_t, const char *, SfRead, void *);
int sf_orc_reader_next(void *, ArrowArray *);
uint64_t sf_orc_reader_memory(void *);
void sf_orc_reader_free(void *);
}
