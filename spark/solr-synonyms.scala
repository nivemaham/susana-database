
spark.read.option("delimiter","\t").csv("private/mimic/dico_typo-50x50.txt").registerTempTable("dic")
val dic = spark.sql("""
  SELECT   concat_ws(',',collect_list(distinct _c1)) as result
  FROM     dic 
  GROUP BY _c0 
  HAVING   count(1) > 1
  """)
dic.coalesce(1).write.mode(org.apache.spark.sql.SaveMode.Overwrite).text("private/syn.txt")

