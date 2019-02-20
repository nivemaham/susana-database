import fr.aphp.eds.spark.postgres.PGUtil

def loadConceptsCsv(csvPath:String, project_id:String, language_id:String, domain_id:String, vocabulary_id:String):Unit = {

  val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
  
  // READ
  //
  
  var df = spark.read.option("inferSchema","true").option("header","true").format("csv").load(csvFile)
  
  // VERIFY
  // - doit contenir toutes les colonnes obligatoires
  // - si code existe alors unique sinon pas charger ce code
  // - écarter les colonnes pas dans le template
  
  var columns = df.columns
  val columnsNeeded = List("local_concept_name","local_concept_code","local_vocabulary","local_frequency")
  verifyColumnsExist(columns, columnsNeeded)
  
  // TRANSFORM
  //
  
  df = df.select("local_concept_name","local_concept_code","local_vocabulary","local_frequency")
  df = df
    .withColumn("m_language_id", language_id)
    .withColumn("m_project_id", project_id)
  
  // LOAD
  //
  
  val pg = PGUtil(spark, url, "/home/natus/spark-postgres-tmp")
  pg.outputBulk("concept", df, 2)

}

def verifyColumnsExist(columns:List(), columnsNeeded:List()):Unit = {

}

// INIT
//

var language_id = "FR"
var project_id = "APHP"
var csvFile = "/home/natus/git/conceptual-mapping/terminologies/aphp/APHP_glims_anabio_loinc.csv"
