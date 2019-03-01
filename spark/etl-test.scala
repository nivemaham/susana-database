import fr.aphp.eds.spark.postgres.PGUtil
import org.apache.spark.sql.{SparkSession,Dataset,Row}


val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=omop"

val a = new PGUtil(spark, url, "/home/mapper/tmp/spark-postgres" )
a.purgeTmp()
val df = a
  .tableDrop("note_tmp")
  .tableCopy("note","note_tmp")
  .inputBulk(query="select * from note", isMultiline=true, numPartitions=4, splitFactor=10, partitionColumn="note_id")

a.outputBulk("note_tmp", df)
  .purgeTmp()

//PGUtil.outputBulkDfScd2(url, password, "concept_copy", "concept_id", "valid_start_date", "valid_end_date", someDF, 50000)
  
//time{
//  PGUtil.getPgQuery(url,"select * from drug_exposure","drug_exposure_id",8,password).write.orc("/tmp/test1")
//}
//time{
//  PGUtil.bulkInputDataframe(spark, url, password, "select * from drug_exposure", "/tmp/test", false)
//}

//System.exit(0)
