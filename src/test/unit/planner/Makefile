subdir=$(top_builddir)/src/test/unit/planner
top_builddir=/Users/mplageman/workspace/gpdb
top_srcdir = $(top_builddir)
include $(top_builddir)/src/Makefile.global

TARGETS=main

include $(top_builddir)/src/backend/mock.mk
main.t: $(MOCK_DIR)/main_mock.o
