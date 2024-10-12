$(OBJS): Makefile
DATA = pg_quota--1.0.sql
EXTENSION = pg_quota
MODULE_big = $(EXTENSION)
OBJS = $(EXTENSION).o
PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
REGRESS = $(patsubst sql/%.sql,%,$(TESTS))
TESTS = $(wildcard sql/*.sql)
include $(PGXS)
