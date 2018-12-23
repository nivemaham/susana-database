import org.apache.spark.sql.functions._
//
// [ETL]
//

object DbUtil {
  def dbPassword(hostname:String, port:String, database:String, username:String ):String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password
    val passwdFile = new java.io.File(scala.sys.env("HOME"), ".pgpass")
    var passwd = ""
    val fileSrc = scala.io.Source.fromFile(passwdFile)
    fileSrc.getLines.foreach{line =>
      val connCfg = line.split(":")
      if (hostname == connCfg(0)
        && port == connCfg(1)
        && database == connCfg(2)
        && username == connCfg(3)
      ) { 
        passwd = connCfg(4)
      }
    }
    fileSrc.close
    passwd
  }

  def passwordFromConn(connStr:String) = {
    // Usage: passwordFromConn("hostname:port:database:username")
    val connCfg = connStr.split(":")
    dbPassword(connCfg(0),connCfg(1),connCfg(2),connCfg(3))
  }
}

//
// [E]
// connect to postgres
//

val connectionStr = "jdbc:postgresql://localhost:5432/mimic?user=natus&currentSchema=omopvocab"
val prop = new java.util.Properties()
prop.put("password", DbUtil.passwordFromConn("localhost:5432:mimic:natus"))
spark.read.jdbc(url=connectionStr,table="concept",columnName="concept_id",lowerBound=0,upperBound=4000000,numPartitions=8,connectionProperties=prop).registerTempTable("concept")
spark.read.jdbc(url=connectionStr,table="concept_synonym",columnName="concept_id",lowerBound=0,upperBound=4000000,numPartitions=8,connectionProperties=prop).registerTempTable("concept_synonym")

//
// [T]
// transform
//

spark.sql("""
   SELECT concept_id   as id,
   concept_name        as concept_name_txt_en,
   domain_id           as domain_id_t,
   vocabulary_id       as vocabulary_id_t, 
   concept_class_id    as concept_class_id_t,
   standard_concept    as standard_concept_t,
   concept_code        as concept_code_t
   FROM concept
""").registerTempTable("cptDF")

spark.sql("""
   SELECT concept_id                           as id,
   collect_list(concept_synonym_name) as concept_synonym_name_txt
   FROM concept_synonym
   GROUP BY concept_id
""").registerTempTable("synDF")

val resultDF = spark.sql("""
  SELECT 
  c.*,
  s.* 
  FROM cptDF c
  LEFT JOIN synDF s USING (id)""")

//
// [L]
// load to solr
//

val options = Map( "collection" -> "gettingstarted", "zkhost" -> "localhost:9983")
resultDF.repartition(32).write.format("solr").options(options).option("commit_within", "20000").option("batch_size", "20000").mode(org.apache.spark.sql.SaveMode.Overwrite).save

System.exit(0)

