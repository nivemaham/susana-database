import fr.aphp.eds.spark.postgres.PGUtil
import org.apache.spark.sql.{SparkSession,Dataset,Row}
import scala.collection.mutable.ListBuffer

def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 100000000 + "s")
    result
}

def getConceptPercent(pg:PGUtil, spark:SparkSession, url:String, tableStat:String, columnStat:String):Dataset[Row]={
  val statQuery = s"select $columnStat as concept_id from $tableStat"
  val df = pg.inputBulk(statQuery).registerTempTable("stats")
  val query = """
  WITH
  tmp as (
      select 
        count(1) ct
      , concept_id 
      from stats 
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
  , CASE WHEN ct > p1 THEN 1 WHEN ct > p2 THEN 2 WHEN ct > p3 THEN 3 ELSE 4 END as m_frequency_value
  from tmp
  cross join total
  """
  val synDF = spark.sql(query)
  synDF
}

//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
val pg = PGUtil(spark, url, "/home/natus/spark-postgres-tmp" )

val strList = List(
      List("omop.person"               ,"gender_concept_id")
     ,List("omop.visit_occurrence"     ,"visit_concept_id")
     ,List("omop.visit_detail"         ,"visit_detail_concept_id")
     ,List("omop.observation"          ,"observation_concept_id")
     ,List("omop.measurement"          ,"measurement_concept_id")
     ,List("omop.condition_occurrence" ,"condition_concept_id")
     ,List("omop.procedure_occurrence" ,"procedure_concept_id")
     ,List("omop.drug_exposure"        ,"drug_concept_id")
);    
 
var count = 0
var query = new ListBuffer[String]() //empty mutable list
for(str <- strList){
  count = count + 1
  val statDF = getConceptPercent(pg, spark, url,  str(0), str(1))
  statDF.registerTempTable(f"stats$count")
  query += f"(select * from stats$count)"
}
val queryUnion = query.mkString(" UNION ")
val result = sql(queryUnion)

pg.sqlExec("create table concept_tmp (concept_id integer not null, m_frequency_value smallint)")
pg.outputBulk("concept_tmp", result, 2)
pg.sqlExec("UPDATE concept set m_frequency_value = concept_tmp.m_frequency_value from concept_tmp where concept.concept_id = concept_tmp.concept_id")
pg.tableDrop("concept_tmp")

System.exit(0)
