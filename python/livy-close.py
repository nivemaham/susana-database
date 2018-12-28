import json, pprint, requests, textwrap
headers = {'Content-Type': 'application/json'}
session_url = 'http://localhost:8998/sessions/'
for i in range(1,100):
    va = session_url+str(i)
    print(va)
    requests.delete(va, headers=headers)
