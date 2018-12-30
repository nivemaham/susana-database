import fr.aphp.eds.spark.postgres.PGUtil
import org.apache.spark.sql.{SparkSession,Dataset,Row}

def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 100000000 + "s")
    result
}

def getConceptPercent(spark:SparkSession, url:String, password:String, tableStat:String, columnStat:String):Dataset[Row]={
  val statQuery = s"select $columnStat as concept_id from $tableStat"
  val df = PGUtil.inputQueryBulkDf(spark, url, password, statQuery, "/tmp/test", false).registerTempTable("person")
  val synDF = spark.sql("""
  WITH
  tmp as (
      select 
        count(1) ct
      , concept_id
      from person
      group by concept_id
      ),
  total as (
  SELECT
     percentile_approx(ct, 0.75) as p1
   , percentile_approx(ct, 0.5) as p2
   , percentile_approx(ct, 0.25) as p3
  from tmp)
  SELECT
    concept_id
  , ct
  , CASE WHEN ct > p1 THEN 1 WHEN ct > p2 THEN 2 WHEN ct > p3 THEN 3 ELSE 4 END as percent
  from tmp
  cross join total
  """)
  synDF
}

//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=omop"
val password = PGUtil.passwordFromConn("localhost:5432:mimic:mapper")

val strList = List(
      List("person","gender_concept_id")
     ,List("visit_occurrence","visit_concept_id")
     ,List("visit_detail","visit_detail_concept_id")
     ,List("observation","observation_concept_id")
//   ,List("measurement","measurement_concept_id")
);    
 
for(str <- strList){
  val statDF = getConceptPercent(spark, url, password, str(0), str(1))
  PGUtil.outputBulkDfScd1(url, password, "m_concept_stats", "concept_id", statDF, 50000)
}

//PGUtil.outputBulkDfScd2(url, password, "concept_copy", "concept_id", "valid_start_date", "valid_end_date", someDF, 50000)
  
//time{
//  PGUtil.getPgQuery(url,"select * from drug_exposure","drug_exposure_id",8,password).write.orc("/tmp/test1")
//}
//time{
//  PGUtil.bulkInputDataframe(spark, url, password, "select * from drug_exposure", "/tmp/test", false)
//}

System.exit(0)
