import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

//
// [ETL]
//

object DbUtil {
  def dbPassword(hostname:String, port:String, database:String, username:String ):String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password
    //val passwdFile = new java.io.File(scala.sys.env("HOME"), ".pgpass")
    val passwdFile = new java.io.File( "/home/mapper/.pgpass")
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

  def getPgQuery(url:String,query:String,partition_column:String,num_partitions:Int,password:String):Dataset[Row]={
    // get min and max for partitioning
    val queryStr = f"($query%s) as tmp"
    val min_max_query = f"(SELECT cast(min($partition_column%s) as bigint), cast(max($partition_column%s) + 1 as bigint) FROM $queryStr%s) AS tmp1"
    print(min_max_query)
    val row  = spark.read.format("jdbc").option("url",url).option("dbtable",min_max_query).option("password",password).load.first
    val lower_bound = row.getLong(0)
    val upper_bound = row.getLong(1)
    // get the partitionned dataset from multiple jdbc stmts
    spark.read.format("jdbc").option("url",url).option("dbtable",queryStr).option("partitionColumn",partition_column).option("lowerBound",lower_bound).option("upperBound",upper_bound).option("numPartitions",num_partitions).option("fetchsize",20000).option("password",password).load
    }
}

//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=omopvocab"
val password = DbUtil.passwordFromConn("localhost:5432:mimic:mapper")
    
DbUtil.getPgQuery(url, "select concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code from concept", "concept_id", 8, password).registerTempTable("concept")
DbUtil.getPgQuery(url, "select concept_id, concept_synonym_name from concept_synonym", "concept_id", 8, password).registerTempTable("concept_synonym")
DbUtil.getPgQuery(url, "select concept_id_1, concept_id_2 from concept_relationship where relationship_id = 'Maps to' and concept_id_1 != concept_id_2", "concept_id_1", 8, password).registerTempTable("concept_relationship")


//
// [T]
// transform
//

spark.sql("""
   SELECT concept_id                 
   ,collect_list(concept_synonym_name) as concept_synonym_name
   FROM concept_synonym
   JOIN concept USING (concept_id)
   WHERE concept_synonym_name != concept_name
   GROUP BY concept_id
""").registerTempTable("synDF")

spark.sql("""
   SELECT cpt1.concept_id                 
   ,collect_list(cpt2.concept_name) as concept_mapped_name
   FROM concept_relationship as cr
   JOIN concept as cpt1 on (cr.concept_id_1 = cpt1.concept_id)
   JOIN concept as cpt2 on (cr.concept_id_2 = cpt2.concept_id)
   GROUP BY cpt1.concept_id
""").registerTempTable("mappedDF")

val resultDF = spark.sql("""
   SELECT concept_id    
   , concept_name        
   , domain_id           
   , vocabulary_id       
   , concept_class_id    
   , standard_concept    
   , concept_code        
   , concept_synonym_name
   , concept_mapped_name
   FROM concept
   LEFT JOIN synDF USING (concept_id)
   LEFT JOIN mappedDF USING (concept_id)
""")

//
// [L]
// load to solr
//

val options = Map( "collection" -> "omop-concept", "zkhost" -> "localhost:9983")
resultDF.repartition(32).write.format("solr").options(options).option("commit_within", "20000").option("batch_size", "20000").mode(org.apache.spark.sql.SaveMode.Overwrite).save

System.exit(0)

