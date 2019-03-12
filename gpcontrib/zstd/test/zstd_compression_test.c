#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>

#include "postgres.h"
#include "cmockery.h"
#include "utils.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#define UNIT_TESTING

#include "../zstd_compression.c"


void
test_constructed_without_compress_type(void **state)
{
	StorageAttributes sa = {};

	EXPECT_ELOG(ERROR);

	/*
	 * We need a try/catch because we expect zstd_constructor to throw ERROR.
	 */
	PG_TRY();
	{
		/*
		 * zstd_constructor is a SQL function, so we need to do some setup to call it
		 * DirectFunctionCall is used as a convenience, since it does some of this setup
		 */
		DirectFunctionCall3Coll(zstd_constructor, NULL, NULL, &sa, false);
		assert_false("zstd_constructor did not throw error");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

void
test_zero_complevel_is_set_to_one(void **state)
{
	StorageAttributes sa = { .comptype = "zstd", .complevel = 0 };
	DirectFunctionCall3Coll(zstd_constructor, NULL, NULL, &sa, false);

	assert_int_equal(1, sa.complevel);
}

void
test_compress_with_dst_size_too_small(void **state)
{
	StorageAttributes sa = { .comptype = "zstd", .complevel = 0 };
	int32 dst_used;
	CompressionState *cs = DirectFunctionCall3Coll(zstd_constructor, NULL, NULL, PointerGetDatum(&sa), BoolGetDatum(false));

	expect_any(ZSTD_compressCCtx, cctx);
	expect_any(ZSTD_compressCCtx, dst);
	expect_any(ZSTD_compressCCtx, dstCapacity);
	expect_any(ZSTD_compressCCtx, src);
	expect_any(ZSTD_compressCCtx, srcSize);
	expect_any(ZSTD_compressCCtx, compressionLevel);
	will_return(ZSTD_compressCCtx, -1);

	expect_any(ZSTD_getErrorCode, code);
	will_return(ZSTD_getErrorCode, ZSTD_error_dstSize_tooSmall);

	DirectFunctionCall6(zstd_compress,
		CStringGetDatum("abcde"), 42,
		NULL, NULL,
		&dst_used, cs);

	/* dst_sz is too small to compress "abcde" so it sets the dst_used to src_sz */
	assert_int_equal(42, dst_used);

	DirectFunctionCall1(zstd_destructor, PointerGetDatum(cs));
}

void
test_compress_throws_error(void **state)
{
	StorageAttributes sa = { .comptype = "zstd", .complevel = 0 };
	int32 dst_used;
	CompressionState *cs = DirectFunctionCall3Coll(zstd_constructor, NULL, NULL, PointerGetDatum(&sa), BoolGetDatum(false));

	expect_any(ZSTD_compressCCtx, cctx);
	expect_any(ZSTD_compressCCtx, dst);
	expect_any(ZSTD_compressCCtx, dstCapacity);
	expect_any(ZSTD_compressCCtx, src);
	expect_any(ZSTD_compressCCtx, srcSize);
	expect_any(ZSTD_compressCCtx, compressionLevel);
	will_return(ZSTD_compressCCtx, -1);

	expect_any(ZSTD_getErrorCode, code);
	will_return(ZSTD_getErrorCode, ZSTD_error_GENERIC);

	EXPECT_ELOG(ERROR);

	PG_TRY();
	{
		DirectFunctionCall6(zstd_compress,
			CStringGetDatum("abcde"), 42,
			NULL, NULL,
			&dst_used, cs);
		assert_false("zstd_compress did not throw error");
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

	DirectFunctionCall1(zstd_destructor, PointerGetDatum(cs));
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const		UnitTest tests[] = {
		unit_test(test_constructed_without_compress_type),
		unit_test(test_zero_complevel_is_set_to_one),
		unit_test(test_compress_with_dst_size_too_small),
		unit_test(test_compress_throws_error)
	};

	MemoryContextInit();

	return run_tests(tests);
}
