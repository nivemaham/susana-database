
spark.read.option("delimiter","\t").csv("private/mimic/dico_typo-50x50.txt").registerTempTable("dic")
val dic = spark.sql("select _c1 || ' => ' || _c0 as resutl from dic")
dic.write.text("private/syn.txt")

