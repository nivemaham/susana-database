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


