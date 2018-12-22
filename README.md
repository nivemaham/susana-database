# omop-concept-solr

Syncs the OMOP concept with a SolR instance

1. OMOP concept
    1. load the OMOP concept table from ATHENA CSV
    2. extends the model with external informations
2. SolR cloud 
    1. install and configure apache SolR
3. Spark
    1. install and configure apache Spark
    2. ETL postgres -> SolR
