from livy import LivySession
from livy.models import SessionKind

class SparkLivy:
    def __new__(cls, category):
        if category == 1:
            return SparkLivyBatch.__new__(SparkLivyBatch, category)
        else:
            return SparkLivySession.__new__(SparkLivySession, category)

class SparkLivyBatch(SparkLivy):
    def __new__(cls, category):
        instance = object.__new__(SparkLivyBatch)
        return instance
    def __init__(self, category):
        print(f"I am Batch")

class SparkLivySession(SparkLivy):
    function_run:str = "quezaco"
    session:LivySession
    def __new__(cls, category):
        instance = object.__new__(SparkLivySession)
        return(instance)
    def __init__(self, category):
        self.session = LivySession(url=LIVY_URL, kind=SessionKind.SPARK, name="omop sync", jars=["/opt/lib/postgresql-42.2.5.jar"], driver_memory="15G", executor_memory="5G")
        print("I am init")
    def start(self):
        self.session.start()
    def run(self, *args):
        code_str = self.generate_run(*args)
        print(code_str)
        self.livySession.run(code_str)
    def run_arbitrary(self, code_str:str):
        try:
            self.session.run(code_str)
        except Exception as e: 
            print(e)
    def generate_run(self, *args):
        runList = []
        for arg in args:
            if type(arg) is str:
                runList.append(f'"{arg}"')
            else:
                runList.append(f'{arg}')
        argsStr = ", ".join(runList)
        return(f"{self.function_run}({argsStr})")
    def stop(self):
        self.session.close()

LIVY_URL = 'http://localhost:8998'
a = SparkLivy(2) 
a.start()
a.run()
a.run_arbitrary("2 + 2")
a.run_arbitrary("2 + 3")
a.stop()

# a yaml file 
# passed to the factory
# produces a class
