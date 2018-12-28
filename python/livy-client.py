from livy import LivySession
from livy.models import SessionKind
LIVY_URL = 'http://localhost:8998'

session = LivySession(url=LIVY_URL,kind=SessionKind.SPARK)
session.start()
session.run("val t = 1+1")
session.close()

