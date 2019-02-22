import fr.aphp.eds.spark.postgres.PGUtil
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

def loadConceptCsv(pg:PGUtil, csvPath:String, m_project_type_id:String, m_language_id:String, domain_id:String, vocabulary_id:String, sep:String, quote:String):Unit = {
  println(f"Begin: $csvPath")

  
  // READ
  //
  
  var df = spark.read
    .option("quote","\"")
    .option("escape",quote)
    .option("header","true")
    .option("delimiter",sep)
    .option("mode","FAILFAST")
    .format("csv").load(csvPath)
  
  // VERIFY
  // - doit contenir toutes les colonnes obligatoires
  // - si code existe alors unique sinon pas charger ce code
  // - écarter les colonnes pas dans le template
  
  var columns = df.columns
  val columnsNeeded = Array("local_concept_name")
  if(verifyColumnsExist(columns, columnsNeeded)){
  
  // TRANSFORM
  //
  if(!verifyColumnsExist(columns, Array("local_concept_code")))
    df = df.withColumn("local_concept_code",lit(null).cast(org.apache.spark.sql.types.StringType))
  if(!verifyColumnsExist(columns, Array("local_frequency")))
    df = df.withColumn("local_frequency",lit(null).cast(org.apache.spark.sql.types.StringType))

  df = df.select("local_concept_name","local_concept_code","local_frequency")
    .withColumnRenamed("local_concept_name","concept_name")
    .withColumnRenamed("local_concept_code","concept_code")
    .withColumnRenamed("local_frequency","m_value_as_number")
    .withColumn("m_language_id", lit(m_language_id))
    .withColumn("m_project_type_id", lit(m_project_type_id))
    .withColumn("domain_id", lit(domain_id))
    .withColumn("vocabulary_id", lit(vocabulary_id))
    .withColumn("m_statistic_type_id", lit("FREQ"))

  var concept_tmp = df.select('concept_name, 'concept_code, 'm_language_id, 'm_project_type_id, 'domain_id, 'vocabulary_id, 'm_statistic_type_id, 'm_value_as_number)
  concept_tmp = verifyColumnNotNull(concept_tmp, "concept_name")
  concept_tmp = verifyColumnUnique(concept_tmp, "concept_name")
  // LOAD
  //
  
  pg.tableDrop("concept_tmp")
  pg.sqlExec("""
    create table concept_tmp 
    (
        concept_name text not null
      , concept_code text
      , m_language_id text
      , m_project_type_id text
      , domain_id text
      , vocabulary_id text
      , m_statistic_type_id text
      , m_value_as_number double precision
    )
    """)

  pg.sqlExec(f"""
    WITH 
    del as (
      delete
      from concept 
      where vocabulary_id = '$vocabulary_id' 
      AND domain_id = '$domain_id'
      AND m_project_id IN (select m_project_id from mapper_project where lower(m_project_type_id) = lower('$m_project_type_id'))
      returning concept_id
    ),
    del2 AS (
      delete from mapper_statistic where m_concept_id in (select concept_id from del)
    ),
    del3 AS (
      delete from concept_synonym where concept_id in (select concept_id from del)
    )
    delete from concept_relationship where concept_id_1 in (select concept_id from del) OR concept_id_2 in (select concept_id from del)
    """)

  pg.outputBulk("concept_tmp", concept_tmp, 2)

  pg.sqlExec("""
    WITH 
    ins as (
      INSERT INTO concept 
      (concept_name, concept_code, m_language_id, m_project_id, domain_id, concept_class_id, vocabulary_id, valid_start_date, valid_end_date)
      SELECT concept_name, coalesce(concept_code,'UNKNOWN'), m_language_id, coalesce(m_project_id, 1) as m_project_id, domain_id, domain_id, vocabulary_id, now()::date, '20990101'
      FROM concept_tmp
      LEFT JOIN mapper_project ON lower(concept_tmp.m_project_type_id) = lower(mapper_project.m_project_type_id)
      RETURNING concept_id, concept_name
    )
      INSERT INTO mapper_statistic 
      (m_concept_id, m_statistic_type_id, m_value_as_number, m_algo_id, m_user_id, m_valid_start_date)
      SELECT concept_id, m_statistic_type_id, m_value_as_number, 4, 10, now()
      FROM ins
      JOIN concept_tmp USING (concept_name)
    """)


  pg.tableDrop("concept_tmp")

  println(f"Loaded: $csvPath")
  }
}

def verifyColumnsExist(columns:Array[String], columnsNeeded:Array[String]):Boolean = {
  var tmp = ""
  for(column <- columnsNeeded){
    if(!columns.contains(column))
      tmp += column + " " + "abscent"
  }
  if(tmp!=""){
    println(columns.mkString(","))
    println(tmp)
    false
  }else{
    true
  }
}

def verifyColumnNotNull(df:Dataset[Row], column:String):Dataset[Row]={
  df.registerTempTable("nullTmp")
  var nulltmp = sql(f"select * from nullTmp where $column IS NULL")
  println(nulltmp.count + " missing rows")
  sql(f"select * from nullTmp where $column IS NOT NULL")
}

def verifyColumnUnique(df:Dataset[Row], column:String):Dataset[Row]={
  val tmp = df.dropDuplicates(column)
  df.except(tmp).show
  val diff = df.count - tmp.count
  if(diff > 0)
    println(f"removed $diff rows")
  tmp
}

// INIT
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
val pg = PGUtil(spark, url, "/home/natus/spark-postgres-tmp")
val TERM_PATH = "/home/natus/git/conceptual-mapping/terminologies/"
var conf = spark.read.option("inferSchema","true").option("header","true").format("csv").load("/home/natus/git/conceptual-mapping/interchuprojet.csv")
var it = conf.rdd.toLocalIterator
while (it.hasNext){
  var value = it.next
  var active     = value.getAs("active").toString  
  var file       = value.getAs("file").toString
  var project    = value.getAs("project").toString
  var domain     = value.getAs("domain").toString
  var vocabulary = value.getAs("vocabulary").toString
  var language   = value.getAs("language").toString
  var mode       = value.getAs("mode").toString
  var sep        = value.getAs("sep").toString
  var quote        = value.getAs("quote").toString
  var csvPath    = TERM_PATH + project.toLowerCase + "/" + file
  if(active=="1")
  loadConceptCsv(pg, csvPath, project, language, domain, vocabulary, sep, quote)
}

