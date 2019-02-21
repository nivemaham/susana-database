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
  , CASE WHEN ct > p1 THEN 4 WHEN ct > p2 THEN 3 WHEN ct > p3 THEN 2 ELSE 1 END as m_frequency_value
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

pg.tableDrop("concept_tmp")
pg.sqlExec("create table concept_tmp (concept_id integer not null, m_frequency_value smallint)")
pg.outputBulk("concept_tmp", result, 2)

pg.sqlExec("""
  UPDATE mapper_statistic 
  set m_value_as_number = concept_tmp.m_frequency_value, m_algo_id = 4, m_user_id = 10, m_valid_start_date=now()
  FROM concept_tmp 
  WHERE mapper_statistic.m_concept_id = concept_tmp.concept_id 
  AND m_statistic_type_id = 'FREQ'
  """)

pg.sqlExec("""
  INSERT INTO mapper_statistic 
  (m_concept_id, m_statistic_type_id, m_value_as_number, m_algo_id, m_user_id, m_valid_start_date)
  SELECT concept_id, 'AVG', m_frequency_value, 4, 10, now()
  FROM concept_tmp
  LEFT JOIN mapper_statistic ON concept_tmp.concept_id = mapper_statistic.m_concept_id
  WHERE mapper_statistic.m_concept_id IS NULL
  """)

pg.tableDrop("concept_tmp")

// VALUES_AVG !!
pg.inputBulk(query=f"""
  select 
  measurement_concept_id as concept_id
  , value_as_number 
  from omop.measurement 
  where measurement_concept_id !=0
  """,  numPartitions=4, partitionColumn="concept_id").registerTempTable("measurement")

pg.inputBulk(query=f"""
  select 
    observation_concept_id as concept_id
  , value_as_number
  , case when value_as_string is not null then 1 else 0 end as value_is_text 
  from omop.observation 
  where observation_concept_id !=0
  """,  numPartitions=4, partitionColumn="concept_id").registerTempTable("observation")

spark.sql("""
  select   concept_id
         , avg(value_as_number) as m_value_avg
         from measurement
         group by concept_id
""").registerTempTable("measDF")

spark.sql("""
  select   concept_id
         , avg(value_as_number) as m_value_avg
         from observation
         group by concept_id
""").registerTempTable("obsDF")

val avgResult = sql("select * from measdf union select * from obsdf")

pg.sqlExec("create table concept_tmp (concept_id integer not null, m_value_avg float)")

pg.outputBulk("concept_tmp", avgResult, 2)

pg.sqlExec("""
  UPDATE mapper_statistic 
  set m_value_as_number = concept_tmp.m_value_avg, m_algo_id = 4, m_user_id = 10, m_valid_start_date=now()
  FROM concept_tmp 
  WHERE mapper_statistic.m_concept_id = concept_tmp.concept_id 
  AND m_statistic_type_id = 'AVG'
  """)

pg.sqlExec("""
  INSERT INTO mapper_statistic 
  (m_concept_id, m_statistic_type_id, m_value_as_number, m_algo_id, m_user_id, m_valid_start_date)
  SELECT concept_id, 'AVG', m_value_avg, 4, 10, now()
  FROM concept_tmp
  LEFT JOIN mapper_statistic ON concept_tmp.concept_id = mapper_statistic.m_concept_id
  WHERE mapper_statistic.m_concept_id IS NULL
  """)

pg.tableDrop("concept_tmp")

System.exit(0)
