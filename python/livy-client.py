from livy import LivySession
from livy.models import SessionKind
LIVY_URL = 'http://localhost:8998'

session = LivySession(url=LIVY_URL,kind=SessionKind.SPARK)
session.start()
session.run("""import fr.aphp.eds.spark.postgres.PGUtil
val url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=omop"
val password = PGUtil.passwordFromConn("localhost:5432:mimic:mapper")
val conn = PGUtil.connOpen(url, password)
conn.close()
1+1""")
session.close()

