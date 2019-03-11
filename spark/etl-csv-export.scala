import fr.aphp.eds.spark.postgres.PGUtil

// EXPORT
val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
val pg = PGUtil(spark, url, "/home/natus/spark-postgres-tmp")
val EXPORT_PATH = "/tmp/export/"

def runJob(project:String):Unit = {

val statQuery = s"""
select 
  c1.concept_id local_concept_id, c1.concept_name local_concept_name, c1.concept_code local_concept_code, c1.domain_id local_domain_id, c1.vocabulary_id local_vocabulary_id
, cr.relationship_id
, c2.concept_id, c2.concept_name, c2.concept_code, c2.domain_id, c2.vocabulary_id, c2.standard_concept
, cr.m_mapping_comment, cr.m_mapping_rate
, mp.m_project_type_id
from concept_relationship cr
join concept c1        on concept_id_1 = c1.concept_id 
join concept c2        on concept_id_2 = c2.concept_id 
join mapper_project mp on c1.m_project_id = mp.m_project_id
where TRUE
AND m_project_type_id = '$project' 
AND m_modif_end_datetime IS NULL
"""

pg.inputBulk(statQuery).write.format("csv").save(EXPORT_PATH)

}
