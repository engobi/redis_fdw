##########################################################################
#
#                foreign-data wrapper for Redis
#
# Copyright (c) 2011, PostgreSQL Global Development Group
#
# This software is released under the PostgreSQL Licence
#
# Author: Dave Page <dpage@pgadmin.org>
#
# IDENTIFICATION
#                 redis_fdw/Makefile
# 
##########################################################################

MODULE_big = redis_fdw
OBJS = redis_fdw.o

EXTENSION = redis_fdw
DATA = redis_fdw##1.0.sql

REGRESS = redis_fdw

EXTRA_CLEAN = sql/redis_fdw.sql expected/redis_fdw.out

SHLIB_LINK += -lhiredis

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/redis_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

