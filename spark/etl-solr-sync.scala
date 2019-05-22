import fr.aphp.eds.spark.postgres.PGUtil
import fr.aphp.eds.spark.solr.MapperConceptSync


val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
val options = Map( "collection" -> "omop-mapper", "zkhost" -> "localhost:9983", "commit_within" -> "20000", "batch_size" -> "20000")

def runJob(concept_id:String):Unit = {

  MapperConceptSync(spark, options, url).transform(Option(concept_id)).sync

}
