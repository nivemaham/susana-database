import fr.aphp.eds.spark.postgres.PGUtil
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


//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
    
// WHEN NONE -> FULL
// WHEN x -> one maj

def runJob(concept_id:String="-1"):Unit = {

val pg = PGUtil(spark, url, "/tmp/spark-postgres-tmp" )

val concept_filter = if(concept_id == "-1") "TRUE" else f" $concept_id = concept_id"
if (concept_id == "-1"){// full
pg.inputBulk(query=f"select concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, m_language_id, m_frequency_id, invalid_reason from concept",  numPartitions=4, partitionColumn="concept_id").registerTempTable("concept")
pg.inputBulk(query=f"select concept_id, concept_synonym_name from concept_synonym",  numPartitions=4, partitionColumn="concept_id").registerTempTable("concept_synonym")
pg.inputBulk(query=f"select concept_id_1, concept_id_2, relationship_id from concept_relationship where concept_id_1 != concept_id_2 AND m_modif_end_datetime IS NULL",  numPartitions=4, partitionColumn="concept_id_1").registerTempTable("concept_relationship")
}else {

pg.input(query=f"select concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, m_language_id, m_frequency_id, invalid_reason from concept where concept_id IN ( $concept_id )").registerTempTable("concept")
pg.input(query=f"select concept_id, concept_synonym_name from concept_synonym where concept_id IN ($concept_id)").registerTempTable("concept_synonym")
pg.input(query=f"select concept_id_1, concept_id_2, relationship_id from concept_relationship where concept_id_1 != concept_id_2 AND m_modif_end_datetime IS NULL AND invalid_reason IS DISTINCT FROM 'D' AND concept_id_1 IN ( $concept_id )").registerTempTable("concept_relationship")

}

//
// [T]
// transform
//

spark.sql("""
   SELECT concept_id                 
   ,collect_list(concept_synonym_name) as concept_synonym_name
   FROM concept_synonym
   JOIN concept USING (concept_id)
   WHERE concept_synonym_name != concept_name
   GROUP BY concept_id
""").registerTempTable("synDF")

spark.sql("""
   SELECT cr.concept_id_1                 as concept_id
   ,collect_list(cpt2.concept_name) as concept_mapped_name
   ,collect_list(cpt3.concept_name) as standard_concept_mapped_name
   ,collect_list(cpt4.concept_name) as non_standard_concept_mapped_name
   ,collect_list(cpt5.concept_name) as concept_relation_name
   FROM concept_relationship as cr
   LEFT JOIN concept as cpt2 on (cr.concept_id_2 = cpt2.concept_id AND lower(cr.relationship_id) IN ('maps to','is a'))
   LEFT JOIN concept as cpt3 on (cr.concept_id_2 = cpt3.concept_id AND lower(cr.relationship_id) IN ('maps to','is a') AND lower(cpt3.standard_concept) IS NOT DISTINCT FROM 's') 
   LEFT JOIN concept as cpt4 on (cr.concept_id_2 = cpt4.concept_id AND lower(cr.relationship_id) IN ('maps to','is a') AND lower(cpt4.standard_concept) IS DISTINCT FROM 's')
   LEFT JOIN concept as cpt5 on (cr.concept_id_2 = cpt5.concept_id AND lower(cr.relationship_id) NOT IN ('maps to','is a') )
   GROUP BY cr.concept_id_1
""").registerTempTable("mappedDF")

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
   , COALESCE(domain_id, 'EMPTY')           as domain_id
   , COALESCE(vocabulary_id, 'EMPTY')       as vocabulary_id
   , COALESCE(concept_class_id, 'EMPTY')    as concept_class_id
   , COALESCE(standard_concept, 'EMPTY')    as standard_concept
   , COALESCE(invalid_reason, 'EMPTY')      as invalid_reason
   , COALESCE(concept_code, 'EMPTY')        as concept_code
   , concept_synonym_name
   , concept_mapped_name
   , COALESCE(m_language_id, 'EMPTY')       as m_language_id
   , COALESCE(m_frequency_id, 'EMPTY')      as m_frequency_id
   , CASE 
     WHEN standard_concept_mapped_name IS NOT NULL THEN 'S' 
     WHEN non_standard_concept_mapped_name IS NOT NULL THEN 'NS' 
     WHEN concept_relation_name IS NOT NULL THEN 'R' 
     ELSE 'EMPTY'
     END as is_mapped
   , COALESCE(local_map_number, 0) as local_map_number
   FROM concept
   LEFT JOIN synDF USING (concept_id)
   LEFT JOIN mappedDF USING (concept_id)
   LEFT JOIN localMapDF USING (concept_id)
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
