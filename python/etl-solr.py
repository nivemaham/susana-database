from SparkLivySession import SparkLivySession

a = SparkLivySession("python/etl-solr.yaml") 
a.start()
a.run("1,2")
a.stop()
