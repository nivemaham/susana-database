import pysolr

# create a connection to a solr server
#zookeeper = pysolr.ZooKeeper("localhost:9983")
#solr = pysolr.SolrCloud(zookeeper, "gettingstarted")
solr = pysolr.Solr('http://localhost:8983/solr/gettingstarted')

# do a search
# similar = solr.more_like_this(q='id:1113060', mltfl='concept_name_txt_en')
results = solr.search('concept_name_txt_en:aids',  **{
    'hl': 'true',
    'fl': 'id',
    'hl.method': 'unified',
    'hl.fragsize': 10,
    'hl.fl': 'concept_name_txt_en',
    'sort': 'score desc',
})

print("Saw {0} result(s).".format(len(results.docs)))
print("Total {0} result(s).".format(results.hits))

#first highlight
for (id, hl) in results.highlighting.items():
    print(f"{id} : {hl}")


# {'raw_response': {'responseHeader': {'zkConnected': True,
# 'status': 0,
# 'QTime': 3,
# 'params': {'q': 'concept_name_txt_en:head',
# 'hl': 'true',
# 'fl': 'id',
# 'hl.fragsize': '10',
# 'hl.method': 'unified',
# 'hl.fl': 'concept_name_txt_en',
# 'wt': 'json'}},
# 'response': {'numFound': 5056,
# 'start': 0,
# 'maxScore': 10.457837,
# 'docs': [{'id': '45877363'},
# {'id': '40788479'},
# {'id': '46237573'},
# {'id': '45673311'},
# {'id': '4058315'},
# {'id': '45676271'},
# {'id': '45639996'},
# {'id': '4247585'},
# {'id': '40307966'},
# {'id': '40438787'}]},
# 'highlighting': {'45877363': {'concept_name_txt_en': ['<em>Head</em>']},
# '40788479': {'concept_name_txt_en': ['<em>Head</em>']},
# '46237573': {'concept_name_txt_en': ['<em>Head</em> falls to side; no attempts to lift <em>head</em>.']},
# '45673311': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '4058315': {'concept_name_txt_en': ['<em>Head</em> lymphangiogram']},
# '45676271': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '45639996': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '4247585': {'concept_name_txt_en': ['Domed <em>head</em>']},
# '40307966': {'concept_name_txt_en': ['<em>Head</em> lymphangiogram']},
# '40438787': {'concept_name_txt_en': ['CT of <em>head</em>']}}},
# 'docs': [{'id': '45877363'},
# {'id': '40788479'},
# {'id': '46237573'},
# {'id': '45673311'},
# {'id': '4058315'},
# {'id': '45676271'},
# {'id': '45639996'},
# {'id': '4247585'},
# {'id': '40307966'},
# {'id': '40438787'}],
# 'hits': 5056,
# 'debug': {},
# 'highlighting': {'45877363': {'concept_name_txt_en': ['<em>Head</em>']},
# '40788479': {'concept_name_txt_en': ['<em>Head</em>']},
# '46237573': {'concept_name_txt_en': ['<em>Head</em> falls to side; no attempts to lift <em>head</em>.']},
# '45673311': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '4058315': {'concept_name_txt_en': ['<em>Head</em> lymphangiogram']},
# '45676271': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '45639996': {'concept_name_txt_en': ['<em>Head</em> and Shoulders']},
# '4247585': {'concept_name_txt_en': ['Domed <em>head</em>']},
# '40307966': {'concept_name_txt_en': ['<em>Head</em> lymphangiogram']},
# '40438787': {'concept_name_txt_en': ['CT of <em>head</em>']}},
# 'facets': {},
# 'spellcheck': {},
# 'stats': {},
# 'qtime': 3,
# 'grouped': {},
# 'nextCursorMark': None}
