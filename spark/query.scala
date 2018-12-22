val options = Map( "collection" -> "gettingstarted", "zkhost" -> "localhost:9983")
val solrDF = spark.read.format("solr").options(options).load
spark.read.format("solr").option("fields","id,concept_name_txt_en").option("query","concept_name_txt_en:sepsis").options(options).load.show
