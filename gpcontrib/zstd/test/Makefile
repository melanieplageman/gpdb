top_builddir=../../..
include $(top_builddir)/src/Makefile.global

TARGETS= zstd_compression

include $(top_builddir)/src/backend/mock.mk

zstd_compression.t: \
	$(MOCK_DIR)/backend/utils/error/elog_mock.o \
	zstd_mock.o
