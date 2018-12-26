import pysolr

def ifnull(var, val):
  if var is None:
    return val
  return var

# create a connection to a solr server
#zookeeper = pysolr.ZooKeeper("localhost:9983")
#solr = pysolr.SolrCloud(zookeeper, "gettingstarted")
solr = pysolr.Solr('http://localhost:8983/solr/omop-concept')

# do a search
print("##\n# KEYWORD SEARCH\n##")
results = solr.search('concept_name:stroke',  **{
    'hl': 'true',
    'fl': 'id',
    'hl.method': 'unified',
    'termVectors': 'true',
    'hl.fragsize': 10,
    'hl.fl': 'concept_name',
    'sort': 'score desc',
})


#first highlight
#for (id, hl) in results.highlighting.items():
#    print(f"{id} : {hl}")
#
#print(f"Saw {len(results.docs)} over {results.hits} results")

# EDISMAX search
# handle multiword synonyms
# handle field query
# handle boosting
print("##\n# KEYWORD SEARCH\n##")
results = solr.search('(suspected neck cancer) AND standard_concept:S',  **{
    'defType': 'edismax',
    'fl': 'id,standard_concept',
    'qf': 'concept_synonym_name^1 concept_name^2',
    'pf3': 'concept_synonym_name^.3 concept_name^6',
    'ps3': '2',
    'sort': 'score desc',
    'hl': 'true',
    'hl.method': 'unified',
    'termVectors': 'true',
    'hl.fragsize': 10,
    'hl.fl': 'concept_name,concept_synonym_name',
    'sow': 'false',
    'rows': 10,
})


#first highlight
for (id, hl) in results.highlighting.items():
    print(f"{id} : {hl}")

print(f"Saw {len(results.docs)} over {results.hits} results")
