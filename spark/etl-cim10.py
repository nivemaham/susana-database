
# ENGLISH TRANSLATION
SEP = " ____ "
spark.read.option("delimiter",",").option("header","true").option("quote",'"').csv("/home/mapper/app/omop-concept-solr/private/df.csv").registerTempTable("df")
result = sql("select row_number() over(order by lib) as id, cd, lib, (length(lib) + " + str(len(SEP)) + ") as nchar, 0 as group_flag from df order by 3 asc").toPandas()

count = 0
group = 0
for row in result.itertuples():
    count += row.nchar + len(SEP)
    if count >= 500:
        group += 1
        count = row.nchar
    result.at[row.Index, 'group_flag'] = group


spark.createDataFrame(result).registerTempTable("dfsg")
df = sql("select concat_ws('" + SEP + "', collect_list(group_flag)) as group_cd, concat_ws('" + SEP + "',collect_list(lib)) as group_lib from dfsg group by group_flag")

from pyspark.sql.functions import udf
from translate import Translator

# Use udf to define a row-at-a-time udf
@udf('string')
# Input/output are both a single double value
def pandas_translate(v):
    return Translator(from_lang="fr",to_lang="en").translate(v)

translated = df.withColumn('group_result', pandas_translate(df.group_lib)).toPandas()

# LOAD CIM10
# create sequence omop_concept_id_seq;
# ALTER TABLE mapper.concept ALTER concept_id SET DEFAULT NEXTVAL('omop_concept_id_seq');
# select setval('omop_concept_id_seq',2000000001);

url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=mapper"
pg = sc._jvm.fr.aphp.eds.spark.postgres.PGUtil.apply(spark._jsparkSession, url, "/tmp/spark-postgres")
spark.read.option("delimiter",",").option("header","true").option("quote",'"').csv("/home/mapper/app/omop-concept-solr/private/df.csv").registerTempTable("cim")
df = sql("""select 
   lib as concept_name
, 'Condition' as domain_id
, 'CIM10' as vocabulary_id
, 'billing code' as concept_class_id
, coalesce(cd, 'UNKNOWN') as concept_code 
, current_date() as valid_start_date
, '2099-01-01' valid_end_date
FROM cim
""")
pg.outputBulk("concept", df._jdf, 4)



# CIM10 MAPPING
pg.inputBulk("select concept_id, concept_code from concept where vocabulary_id = 'ICD10CM'", False, 2, 1, "concept_id").registerTempTable("omop_icd")
pg.inputBulk("select concept_id, concept_code from concept where vocabulary_id = 'CIM10'", False, 2, 1, "concept_id").registerTempTable("cim")
sql("select concept_id, replace(concept_code,'.','') as concept_code from omop_icd").registerTempTable("icd")
sql("SELECT cim.concept_id as concept_id_1, icd.concept_id concept_id_2 FROM cim JOIN icd USING (concept_code)").registerTempTable("mapped")
icdmap = sql("(select concept_id_1, concept_id_2, 'Maps To' as relationship_id, current_date() valid_start_date, '2099-01-01' valid_end_date from mapped) UNION ALL (select concept_id_1 as concept_id_2, concept_id_2 as concept_id_1, 'Mapped From' as relationship_id, current_date() valid_start_date, '2099-01-01' valid_end_date from mapped)")

# ENRICH MAPPING
pg.inputBulk("select concept_id_1, concept_id_2, relationship_id from concept_relationship", False, 4, 5, "concept_id_1").registerTempTable("omop_rela")
othermap = sql("""WITH
icd as (
select 
 concept_id_1 as src
,concept_id_2 as targ
from omop_rela 
join cim on concept_id = concept_id_1
)
select
  src as concept_id_1
, concept_id_2
, relationship_id
, current_date() valid_start_date
, '2099-01-01' valid_end_date
from omop_rela
join icd on targ = concept_id_1
""")

pg.outputBulk("concept_relationship", icdmap._jdf, 4)
pg.outputBulk("concept_relationship", othermap._jdf, 4)

# purge
pg.purgeTmp()
