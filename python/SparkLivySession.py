from livy import LivySession
from livy.models import SessionKind
import yaml
import logging
import sys


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
    bullet_number: int = None
    bullet: int = 0

    def __init__(self,  yamlFilePath, bullet_number=30):
        with open(yamlFilePath) as stream:
            conf = yaml.safe_load(stream)
        self.url = conf["url"]
        self.name = conf["name"]
        self.jars = conf["jars"]
        self.driver_memory = conf["driver_memory"]
        self.executor_memory = conf["executor_memory"]
        self.code_job = conf["code"]["job"]
        self.bullet_number = bullet_number
        with open(conf["code"]["init"], 'r') as myfile:
            self.code_init = myfile.read()


    def checkBullets(self):
        logging.info("checking bullets")
        if( self.bullet > self.bullet_number ):
            logging.warning("too many bullets, dropping the session")
            self.stop()
            self.startSession()
            self.bullet = 0

    def checkState(self):
        logging.info("checking state")
        if( self.session is None or self.session.session_id is None ):
            logging.info("No session yet ")
            self.startSession()


    def startSession(self):
        logging.info("creating  a session")
        self.session = LivySession(url=self.url, kind=SessionKind.SPARK, name=self.name, jars=self.jars, driver_memory=self.driver_memory, executor_memory=self.executor_memory)
        self.session.start()
        self.session.run(self.code_init)

    def run(self, *args):
        self.checkBullets()
        self.checkState()
        code_str = self.generate_run(*args)
        logging.info(code_str)
        try:
            logging.warning("running code: %s" % code_str)
            self.session.run(code_str)
            self.bullet = self.bullet + 1
        except:
            e = sys.exc_info()[0]
            logging.error(e)
            self.stop()
            self.run(args)

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
        try:
            self.session.close()
        except:
            logging.warn("session already dead")

        self.session = None
