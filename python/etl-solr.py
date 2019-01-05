from SparkLivySession import SparkLivySession

a = SparkLivySession("python/etl-solr.yaml") 
a.start()
a.run()
a.stop()
