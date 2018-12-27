OMOP_SCHEMA=omopvocab
PG_CONF=host=localhost dbname=mimic user=mapper
OMOP=$(PG_CONF) options=--search_path=$(OMOP_SCHEMA)
ATHENA_FOLDER=private/athena
SOLR_FOLDER=/opt/solr/current


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
	sh solr/synonyms/syn-build.sh
	$(SOLR_FOLDER)/bin/solr delete -c omop-concept
	$(SOLR_FOLDER)/server/scripts/cloud-scripts/zkcli.sh -cmd clear -z "localhost:9983" /configs/omop-concept
	$(SOLR_FOLDER)/bin/solr create -c omop-concept -d solr/configsets/omop -n omop-concept -p 8983

solr-start:
	$(SOLR_FOLDER)/bin/solr start -e cloud -m 4G -all -noprompt

solr-stop:
	$(SOLR_FOLDER)/bin/solr stop -all

solr-restart: solr-stop solr-start

spark-load:
	spark-shell --driver-class-path /opt/lib/postgresql-42.2.5.jar  --jars /opt/lib/spark-solr-3.7.0-SNAPSHOT-shaded.jar --master local[20] --driver-memory=5G  --executor-memory=3G  -i spark/etl.scala 


