OMOP_SCHEMA=omopvocab
PG_CONF=host=localhost dbname=mimic user=natus
OMOP=$(PG_CONF) options=--search_path=$(OMOP_SCHEMA)
ATHENA_FOLDER=private/athena

install-omop:
	psql "$(PG_CONF)" -c 'DROP SCHEMA IF EXISTS "$(OMOP_SCHEMA)" CASCADE;'
	psql "$(PG_CONF)" -c 'CREATE SCHEMA "$(OMOP_SCHEMA)";'
	echo "$(OMOP)"
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql ddl.txt"
	(cd $(ATHENA_FOLDER) && psql "$(OMOP)" -f "omop/build/omop_vocab_load.sql")
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql constraints.txt"
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql pk indexes.txt"

