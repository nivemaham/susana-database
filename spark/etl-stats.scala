import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.util.Properties
import java.sql._
import java.io.ByteArrayInputStream
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
//
// [ETL]
//

object DbUtil extends java.io.Serializable {
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

  def getConceptPercent(url:String,query:String,partition_column:String,num_partitions:Int,password:String, tableStat:String, columnStat:String):Dataset[Row]={
    val statQuery = f"select $columnStat%s as concept_id from $tableStat%s"
    getPgQuery(url, statQuery, "concept_id", 8, password).registerTempTable("person")
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

  def prepareCsv(df:Dataset[Row]):Dataset[Row]={
    import scala.collection.mutable.ListBuffer
    val sqlStr =  new ListBuffer[String]()
    for(sch <- df.schema){
      val colname = "`" + sch.name + "`"
      if(sch.dataType == StringType){
        sqlStr+=(s"""concat('"',regexp_replace(coalesce(${colname},''),'"','""'),'"') as ${colname}""")
      }else if(List(IntegerType, LongType, DoubleType, FloatType, DateType, TimestampType).contains(sch.dataType)){
        sqlStr+=(s"""coalesce(${colname},'') as ${colname}""")
      }else{
      sqlStr+=(colname)
      }
    }
    df.registerTempTable("df823")
    sql("SELECT " + sqlStr.toList.mkString(",") + " from df823")
  }

  def pgCopy(url:String, password:String, table:String, df:Dataset[Row], batchsize:Int) = {
    val dialect = JdbcDialects.get(url)
    val copyColumns =  df.schema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")

    prepareCsv(df).rdd.mapPartitions(
      batch => 
      {
      val conn = connOpen(url, password)
      batch.grouped(batchsize).foreach{
         session => 
         {
           // TODO: the below regex may remove bracket when preceding with newline
           val str = session.mkString("\n").replaceAll("(?m)^\\[|\\]$","") 
           val targetStream = new ByteArrayInputStream(str.getBytes());
           val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection] );
           copyManager.copyIn(s"""COPY $table ($copyColumns) FROM STDIN WITH CSV DELIMITER ',' ESCAPE '"' QUOTE '"' """, targetStream  );
         }
      }
    conn.close()
    batch 
    }).take(1)
  }

  // from https://stackoverflow.com/questions/6958965/how-to-copy-a-data-from-file-to-postgresql-using-jdbc
 // def pgCopy(conn:Connection, table:String, df:Dataset[Row])={
 //   val str = df.collect.mkString("\n").replaceAll("[\\[\\]]","")
 //   val targetStream = new ByteArrayInputStream(str.getBytes());
 //   val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection] );
 //   copyManager.copyIn(s"""COPY $table FROM STDIN WITH CSV DELIMITER ',' ESCAPE '"' QUOTE '"' """, targetStream  );
 // }

  def connOpen(url:String,password:String):Connection = {
    val prop = new Properties()
    prop.put("password",password)
    val dbc: Connection = DriverManager.getConnection(url, prop)
    dbc
  }

  def connClose(conn:Connection) = {
    conn.close()
  }


  def truncate(conn:Connection, table:String)={
    val st: PreparedStatement = conn.prepareStatement(s"TRUNCATE TABLE $table")
    st.executeUpdate()
  }

  def scd1(conn:Connection, table:String, key:String, rddSchema:StructType)={
    val updSet =  rddSchema.fields.filter(x => !key.equals(x.name)).map(x => s"${x.name} = tmp.${x.name}").mkString(",")
    val updIsDistinct =  rddSchema.fields.filter(x => !key.equals(x.name)).map(x => s"tmp.${x.name} IS DISTINCT FROM tmp.${x.name}").mkString(" OR ")
    val upd = s"""
    UPDATE $table as targ
    SET $updSet
    FROM ${table}_tmp as tmp
    WHERE TRUE
    AND targ.$key = tmp.$key
    AND ($updIsDistinct)
    """
    val st: PreparedStatement = conn.prepareStatement(upd)
    st.executeUpdate()

    val insSet =  rddSchema.fields.map(x => s"${x.name}").mkString(",")
    val insSetTarg =  rddSchema.fields.map(x => s"tmp.${x.name}").mkString(",")
    val ins = s"""
    INSERT INTO $table ($insSet)
    SELECT $insSetTarg
    FROM ${table}_tmp as tmp
    LEFT JOIN $table as targ USING ($key)
    WHERE TRUE
    AND targ.$key IS NULL
    """
    conn.prepareStatement(ins).executeUpdate()

//    UPDATE eds.nda_mouv_ufr_hegp as final
//SET d8_fin=now(), i2b2_action=-1
//WHERE  NOT EXISTS (SELECT 1 FROM eds.nda_mouv_ufr_hegp_tmp tmp
//WHERE tmp.id_mouv_ufr = final.id_mouv_ufr AND tmp.source = final.source)
  }

}


def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
}
//
// [E]
// connect to postgres
//

val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=omop"
val password = DbUtil.passwordFromConn("localhost:5432:mimic:mapper")


// val strList = List(
//    List("person","gender_concept_id")
//    //,List("measurement","measurement_concept_id")
//   ,List("visit_occurrence","visit_concept_id")
//   ,List("visit_detail","visit_detail_concept_id")
//   ,List("observation","observation_concept_id")
// );    
// 
// val conn = DbUtil.connOpen(url, password)
// DbUtil.truncate(conn, "m_concept_stats_tmp")
// for(str <- strList){
//   val statDF = DbUtil.getConceptPercent(url, "select gender_concept_id as concept_id from person", "concept_id", 8, password, str(0), str(1))
//   statDF.write.format("jdbc").mode(org.apache.spark.sql.SaveMode.Append).option("url",url).option("password",password).option("batchsize",20000).option("dbtable", "m_concept_stats_tmp").save()
//   statDF.show
//   time{pgCopy(url, password, "m_concept_stats", statDF)}
//  // DbUtil.scd1(conn, "m_concept_stats", "concept_id", statDF.schema)
// }

val conn = DbUtil.connOpen(url, password)
val df = DbUtil.getPgQuery(url, "select * from concept_relationship LIMIT 100000", "concept_id_1", 8, password).repartition(8).cache()
df.show
DbUtil.truncate(conn, "test_tmp")
time{DbUtil.pgCopy(url, password, "test_tmp", df, 30000)}
//time{ df.write.format("jdbc").mode(org.apache.spark.sql.SaveMode.Overwrite).option("url",url).option("password",password).option("batchsize",20000).option("dbtable", "test_tmp").save() }

DbUtil.connClose(conn)

System.exit(0)
