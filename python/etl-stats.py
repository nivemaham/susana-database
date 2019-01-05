from SparkLivySession import SparkLivySession

a = SparkLivySession("python/etl-stats.yaml") 
a.start()
a.run()
a.stop()
