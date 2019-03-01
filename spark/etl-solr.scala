// PRINCIPLE
// For concept and each language the fields are present:
// - concept_name: the concept_name in it's original langage
// - concept_synonym_name_lg: the concept_synonym's list including the concept_name
// - concept_mapped_name_lg: the mapped_concept_synonyms mapped list
// - concept_hierarchy_name_lg: the hierarchy list labels
// Therefore,
// when an english label maps to an other english label 
//  we can direct all the _en labels to all _en fields
//  we can direct all the _en labels to all _en_lg fields
// we can also translate it to all language
//  we can then direct all the _lg labels to all _lg fields
// Identically,
// when a french label maps to an other english label 
//  we can direct all the _fr labels to all _fr fields
//  we can direct all the _fr labels to all _fr_lg fields
// we can also translate it to all language
//  we can then direct all the _lg labels to all _lg fields
// BASICALY
// each call triggers 3 searches:
// 1. intra language search
// 2. translated search
// 3. bi-lingual search
// TECHNICALY
// This results in [fr->en]:
// 1.  f.fr.qf=concept_name_fr concept_synonym_name_fr concept_mapped_name_fr concept_hierarchy_name_fr
// 2. &f.tr_en.qf=concept_name_en concept_synonym_name_en concept_mapped_name_en concept_hierarchy_name_en
// 3. &f.fr_en.qf=concept_name_fr_en concept_synonym_name_fr_en concept_mapped_name_fr_en concept_hierarchy_name_fr_en
// query: fr:(bonjour monde) tr_en:(hello world) fr_en:(bonjour monde)
//
// This results in [fr->fr]:
// 1.  f.fr.qf=concept_name_fr concept_synonym_name_fr concept_mapped_name_fr concept_hierarchy_name_fr
// 2. &f.tr_en.qf=concept_name_en concept_synonym_name_en concept_mapped_name_en concept_hierarchy_name_en
// 3. &f.fr_en.qf=concept_name_fr_en concept_synonym_name_fr_en concept_mapped_name_fr_en concept_hierarchy_name_fr_en
// query: fr:(bonjour monde) tr_en:(hello world) fr_en:(bonjour monde)
//
//
// [ETL]
//

import fr.aphp.eds.spark.postgres.PGUtil

//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"

val pg = PGUtil(spark, url, "/tmp/spark-postgres-tmp" )
    
val equivalent = " 'maps to','is a', 'mapped from' "

def runJob(concept_id:String="-1"):Unit = {

var concept_query = f"""
  select concept_id
  , concept_name
  , domain_id
  , vocabulary_id
  , concept_class_id
  , standard_concept
  , concept_code
  , m_language_id
  , invalid_reason 
  , m_project_id 
  from concept
  """

var project_query = f"""
  select m_project_id
  , m_project_type_id
  from mapper_project
  """

var avg_query = f"""
  select m_concept_id as concept_id
  , m_value_as_number as m_value_avg
  from mapper_statistic
  where m_statistic_type_id = 'AVG'
  """

var freq_query = f"""
  select m_concept_id as concept_id
  , m_value_as_number as m_frequency_value
  from mapper_statistic
  where m_statistic_type_id = 'FREQ'
  """

var concept_synonym_query = f"""
  select 
    concept_id
  , concept_synonym_name 
  , language_concept_id
  from concept_synonym
  """

var concept_relationship_query = f"""
  select 
    concept_id_1
  , concept_id_2
  , relationship_id 
  from concept_relationship 
  where concept_id_1 != concept_id_2 
  AND m_modif_end_datetime IS NULL
  """

pg.input(query=concept_query + f" where concept_id IN ( $concept_id )").registerTempTable("concept")
pg.input(query=project_query).registerTempTable("project")
pg.input(query=avg_query + f" and m_concept_id IN ( $concept_id )").registerTempTable("stats_avg")
pg.input(query=freq_query + f" and m_concept_id IN ( $concept_id )").registerTempTable("stats_freq")
pg.input(query=concept_synonym_query + f" where concept_id IN ($concept_id)").registerTempTable("concept_synonym")
pg.input(query=concept_relationship_query + f" AND invalid_reason IS DISTINCT FROM 'D' AND concept_id_1 IN ( $concept_id )").registerTempTable("concept_relationship")



//
// [T]
// transform
//

spark.sql("""
   SELECT concept_id                 
   , collect_list(concept_synonym_name) as concept_synonym_name_en
   FROM concept_synonym
   JOIN concept USING (concept_id)
   WHERE concept_synonym.concept_synonym_name != concept.concept_name
   AND concept_synonym.language_concept_id = 4180186 -- ENGLISH
   GROUP BY concept_id
""").registerTempTable("dfsynen")

spark.sql("""
   SELECT concept_id                 
   , collect_list(concept_synonym_name) as concept_synonym_name_fr
   FROM concept_synonym
   JOIN concept USING (concept_id)
   WHERE concept_synonym_name != concept_name
   AND concept_synonym.language_concept_id = 4180190 -- ENGLISH
   GROUP BY concept_id
""").registerTempTable("dfsynfr")

spark.sql("""
   SELECT cr.concept_id_1                 as concept_id
   ,collect_list(cpt3.concept_name) as standard_concept_mapped_name
   ,collect_list(cpt4.concept_name) as non_standard_concept_mapped_name
   ,collect_list(cpt5.concept_name) as concept_relation_name
   FROM concept_relationship as cr
   LEFT JOIN concept as cpt3 on (cr.concept_id_2 = cpt3.concept_id AND lower(cr.relationship_id) IN ('maps to','is a') 
     AND lower(cpt3.standard_concept) IS NOT DISTINCT FROM 's') 
   LEFT JOIN concept as cpt4 on (cr.concept_id_2 = cpt4.concept_id AND lower(cr.relationship_id) IN ('maps to','is a') 
     AND lower(cpt4.standard_concept) IS DISTINCT FROM 's')
   LEFT JOIN concept as cpt5 on (cr.concept_id_2 = cpt5.concept_id AND lower(cr.relationship_id) NOT IN ('maps to','is a') )
   GROUP BY cr.concept_id_1
""").registerTempTable("mappedDF")

spark.sql(f"""
   SELECT cr.concept_id_1                 as concept_id
   ,collect_list(cpt.concept_name) as concept_mapped_name_en
  FROM concept_relationship as cr
   LEFT JOIN concept as cpt on (cr.concept_id_2 = cpt.concept_id AND lower(cr.relationship_id) IN ($equivalent))
  WHERE cpt.m_language_id IS NULL OR cpt.m_language_id = 'EN'
  GROUP BY cr.concept_id_1
""").registerTempTable("dfmapen")

spark.sql(f"""
   SELECT 
     cr.concept_id_1                 as concept_id
   , collect_list(cpt.concept_name) as concept_mapped_name_fr
  FROM concept_relationship as cr
   LEFT JOIN concept as cpt on (cr.concept_id_2 = cpt.concept_id AND lower(cr.relationship_id) IN ($equivalent))
  WHERE cpt.m_language_id = 'FR'
  GROUP BY cr.concept_id_1
""").registerTempTable("dfmapfr")

// concepts already mapped to local concepts
spark.sql("""
  SELECT concept_id_2 as concept_id
  , count(1) as local_map_number
  FROM concept_relationship
  WHERE concept_id_1 >= 2000000000
  group by concept_id_2
""").registerTempTable("localMapDF")

val resultDF = spark.sql("""
   SELECT concept_id as id
   , concept_id    
   , COALESCE(concept_name, 'EMPTY')        as concept_name
   , CASE WHEN m_language_id IS NULL THEN COALESCE(concept_name, 'EMPTY') ELSE null END  as concept_name_en
   , CASE WHEN m_language_id = 'FR'  THEN COALESCE(concept_name, 'EMPTY') ELSE null END  as concept_name_fr
   , CASE WHEN m_language_id = 'FR'  THEN COALESCE(concept_name, 'EMPTY') ELSE null END  as concept_name_fr_en
   , COALESCE(domain_id, 'EMPTY')           as domain_id
   , COALESCE(vocabulary_id, 'EMPTY')       as vocabulary_id
   , COALESCE(concept_class_id, 'EMPTY')    as concept_class_id
   , COALESCE(standard_concept, 'EMPTY')    as standard_concept
   , COALESCE(invalid_reason, 'EMPTY')      as invalid_reason
   , COALESCE(concept_code, 'EMPTY')        as concept_code
   , concept_synonym_name_en
   , concept_synonym_name_fr
   , concept_synonym_name_fr as concept_synonym_name_fr_en
   , concept_mapped_name_en
   , concept_mapped_name_fr
   , concept_mapped_name_fr as concept_mapped_name_fr_en
   , COALESCE(m_language_id, 'EN')       as m_language_id
   , m_frequency_value   as frequency
   , CASE 
     WHEN standard_concept_mapped_name IS NOT NULL THEN 'S' 
     WHEN non_standard_concept_mapped_name IS NOT NULL THEN 'NS' 
     WHEN concept_relation_name IS NOT NULL THEN 'R' 
     ELSE 'EMPTY'
     END as is_mapped
   , COALESCE(local_map_number, 0) as local_map_number
   , m_value_avg as value_avg
   , m_value_avg is not null value_is_numeric
   , m_project_type_id
   FROM concept
   LEFT JOIN mappeddf USING (concept_id)
   LEFT JOIN dfsynen USING (concept_id)
   LEFT JOIN dfsynfr USING (concept_id)
   LEFT JOIN dfmapen USING (concept_id)
   LEFT JOIN dfmapfr USING (concept_id)
   LEFT JOIN localMapDF USING (concept_id)
   LEFT JOIN stats_avg USING (concept_id)
   LEFT JOIN stats_freq USING (concept_id)
   LEFT JOIN project USING (m_project_id)
""")

//
// [L]
// load to solr
//


val options = Map( "collection" -> "omop-concept", "zkhost" -> "localhost:9983")

if(concept_id == "-1")
  resultDF.repartition(32).write.format("solr").options(options).option("commit_within", "20000").option("batch_size", "20000").mode(org.apache.spark.sql.SaveMode.Overwrite).save
else
  resultDF.write.format("solr").options(options).option("commit_within", "2").option("batch_size", "20000").mode(org.apache.spark.sql.SaveMode.Overwrite).save
}
