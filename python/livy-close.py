import json, pprint, requests, textwrap
headers = {'Content-Type': 'application/json'}
session_url = 'http://localhost:8998/sessions/'
#for i in range(1,100):
#    va = session_url+str(i)
#    print(va)
#    requests.delete(va, headers=headers)

data = {'kind': 'spark', 'jars': ["/opt/lib/spark-postgres-0.0.1-SNAPSHOT.jar"]}
headers = {'Content-Type': 'application/json'}
r = requests.post(session_url, data=json.dumps(data), headers=headers)
r.json()
