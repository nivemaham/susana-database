from livy import LivySession
from livy.models import SessionKind
import yaml


class SparkLivySession():
    session:LivySession  = None
    url: str = None
    name: str = None
    jars: [] = []
    driver_memory: str = None
    executor_memory: str = None
    code_job: str = None
    code_init: str = None
    code_job: str = None

    def __init__(self,  yamlFilePath):
        with open(yamlFilePath) as stream:
            conf = yaml.safe_load(stream)
        self.url = conf["url"]
        self.name = conf["name"]
        self.jars = conf["jars"]
        self.driver_memory = conf["driver_memory"]
        self.executor_memory = conf["executor_memory"]
        self.code_job = conf["code"]["job"]
        with open(conf["code"]["init"], 'r') as myfile:
            self.code_init = myfile.read()

        self.session = LivySession(url=self.url, kind=SessionKind.SPARK, name=self.name, jars=self.jars, driver_memory=self.driver_memory, executor_memory=self.executor_memory)

    def start(self):
        self.session.start()
        self.session.run(self.code_init)

    def run(self, *args):
        code_str = self.generate_run(*args)
        self.session.run(code_str)

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
        return(f"{self.code_job}({argsStr})")

    def stop(self):
        self.session.close()

a = SparkLivySession("python/etl-stats.yaml") 
a.start()
a.run()
a.stop()

