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
- use python3.6+


## Spark
- add the projects jars into `SPARK_HOME`/jars/ (this is a livy limitation)

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

