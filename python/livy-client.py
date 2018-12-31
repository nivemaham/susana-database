from livy import LivySession
from livy.models import SessionKind
LIVY_URL = 'http://localhost:8998'

session = LivySession(url=LIVY_URL,kind=SessionKind.SPARK, name="omop sync", jars=["/opt/lib/postgresql-42.2.5.jar"], driver_memory="15G", executor_memory="5G")
session.start()
with open('spark/etl-stats.scala', 'r') as myfile:
    data=myfile.read()
    session.run(data)
session.close()

