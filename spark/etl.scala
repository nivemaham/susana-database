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
val df = spark.read.jdbc(connectionStr,"vocabulary",prop)
df.show

//
// [T]
// transform
//

//
// [L]
// connect to solr
//


System.exit(0)
