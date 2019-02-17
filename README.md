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

# Configuration

## Python
- install the requirements
- install pyLivy from [pyLivy](https://github.com/EDS-APHP/pylivy)
- use python3.6+

## Solr
In order to make zookeeper able to ingest large configurations such synonyms
- add SOLR_OPTS="$SOLR_OPTS -Djute.maxbuffer=0x9fffff" to the '$SOLR_HOME/bin/solr.in.sh'


## Spark
- define `$SPARK_HOME` linux env variable

## Livy
Livy configuration can be found: `$LIVY_HOME`/conf/livy.conf

## Ulimit
You will need at least 65000 open files to make spark and solr work fine.

### At runtime
sudo bash
ulimit -n 8192
sudo - <yourUser>


## With reboot
Edit the  /etc/security/limits.conf file
Add:
*         hard    nofile      65000
*         soft    nofile      65000
root      hard    nofile      65000
root      soft    nofile      65000

# Spark Libraries
The spark library are loaded thought apache livy. They are specified into a
yaml file and loaded from the pylivy library.

## spark-solr
Clone the [spark-postgres](https://github.com/parisni/spark-solr)
Compile it and move the shaded jar into some place.

## spark-postgres
Clone the [spark-postgres](https://github.com/EDS-APHP/spark-postgres)
Compile it and move the shaded jar into some place.

