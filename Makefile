OMOP_SCHEMA=omopvocab
PG_CONF=host=localhost dbname=mimic user=natus
OMOP=$(PG_CONF) options=--search_path=$(OMOP_SCHEMA)
ATHENA_FOLDER=private/athena


postgres-create:
	psql "$(PG_CONF)" -c 'DROP SCHEMA IF EXISTS "$(OMOP_SCHEMA)" CASCADE;'
	psql "$(PG_CONF)" -c 'CREATE SCHEMA "$(OMOP_SCHEMA)";'
	echo "$(OMOP)"
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql ddl.txt"
	psql "$(OMOP)" -f "omop/build/mimic-omop-alter.sql"
	psql "$(OMOP)" -f "omop/build/omop_vocab_load.sql"
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql constraints.txt"
	psql "$(OMOP)" -f "omop/build/OMOP CDM postgresql pk indexes.txt"


solr-create:
	solr delete -c omop
	solr create -c omop -d solr/configsets/omop -n omop -p 8983


spark-load:
	spark-shell --driver-class-path /opt/lib/postgresql-42.2.5.jar  --jars /opt/lib/spark-solr-3.7.0-SNAPSHOT-shaded.jar --master local[20] --driver-memory=5G  --executor-memory=3G  -i spark/etl.scala 


