EXTENSION   = paimon_slot
DATA        = paimon_slot--1.0.sql
REGRESS     = paimon_slot
REGRESS_OPTS = --inputdir=test --outputdir=test

PG_CONFIG  ?= pg_config
PGXS       := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Stage the extension where the test cluster's extension_control_path can find it, so
# `make installcheck` needs no writes to the system PostgreSQL share directory.  The
# <dir>/extension/ layout is required by extension_control_path.
install-local:
	mkdir -p _install/extension
	cp $(EXTENSION).control $(DATA) _install/extension/
.PHONY: install-local
