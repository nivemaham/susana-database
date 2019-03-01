
//
// [L]
// load to solr
//

// configure the access
val options = Map( "collection" -> "omop-concept", "zkhost" -> "localhost:9983")
// use export to boost the retrieval
// choose fields and the query

val df = spark.read.format("solr").options(options)
.option("request_handler", "/export")
.option("solr.params", "defType=edismax&qf=concept_name^5 synonym_concept_name^3")
.option("fields","concept_id")
.option("query","death").load

df.registerTempTable("df")
sql("select count(1) from df").show

