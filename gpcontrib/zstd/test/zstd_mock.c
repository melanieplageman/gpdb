#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include <zstd.h>
#include <zstd_errors.h>

size_t
ZSTD_compressCCtx(ZSTD_CCtx* cctx, void* dst, size_t dstCapacity,
	const void* src, size_t srcSize, int compressionLevel)
{
	check_expected(cctx);
	check_expected(dst);
	check_expected(dstCapacity);
	check_expected(src);
	check_expected(srcSize);
	check_expected(compressionLevel);
	return (size_t)mock();
}


ZSTD_ErrorCode
ZSTD_getErrorCode(size_t code)
{
	check_expected(code);
	return (ZSTD_ErrorCode)mock();
}
