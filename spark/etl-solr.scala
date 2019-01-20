import fr.aphp.eds.spark.postgres.PGUtil

//
// [ETL]
//


//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=mapper"
    

val pg = PGUtil(spark, url, "spark-postgres-tmp" )
pg.inputBulk(query="select concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code from concept",  numPartitions=4, partitionColumn="concept_id").registerTempTable("concept")
pg.inputBulk(query="select concept_id, concept_synonym_name from concept_synonym",  numPartitions=4, partitionColumn="concept_id").registerTempTable("concept_synonym")
pg.inputBulk(query="select concept_id_1, concept_id_2 from concept_relationship where relationship_id = 'Maps to' and concept_id_1 != concept_id_2",  numPartitions=4, partitionColumn="concept_id_1").registerTempTable("concept_relationship")


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
   SELECT cpt1.concept_id                 
   ,collect_list(cpt2.concept_name) as concept_mapped_name
   FROM concept_relationship as cr
   JOIN concept as cpt1 on (cr.concept_id_1 = cpt1.concept_id)
   JOIN concept as cpt2 on (cr.concept_id_2 = cpt2.concept_id)
   GROUP BY cpt1.concept_id
""").registerTempTable("mappedDF")

val resultDF = spark.sql("""
   SELECT concept_id    
   , concept_name        
   , domain_id           
   , vocabulary_id       
   , concept_class_id    
   , standard_concept    
   , concept_code        
   , concept_synonym_name
   , concept_mapped_name
   FROM concept
   LEFT JOIN synDF USING (concept_id)
   LEFT JOIN mappedDF USING (concept_id)
""")

//
// [L]
// load to solr
//


val options = Map( "collection" -> "omop-concept", "zkhost" -> "localhost:9983")
resultDF.repartition(32).write.format("solr").options(options).option("commit_within", "20000").option("batch_size", "20000").mode(org.apache.spark.sql.SaveMode.Overwrite).save

def runJob(){}
