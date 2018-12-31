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
results = solr.search('concept_mapped_name:[* TO *]',  **{
    'hl': 'true',
    'fl': '*',
})


#first highlight
#for result in results.docs:
#    print(f"{result}")
#
#print(f"Saw {len(results.docs)} over {results.hits} results")

# EDISMAX search
# handle multiword synonyms
# handle field query
# handle boosting
print("##\n# KEYWORD SEARCH\n##")
results = solr.search('(Meningeal irritation) AND standard_concept:S',  **{
    'defType': 'edismax',
    'fl': '*',
    'qf': 'concept_synonym_name^1 concept_name^2 concept_mapped_name^1',
    'pf3': 'concept_synonym_name^3 concept_name^6 concept_mapped_name^3',
    'ps3': '2',
    'sort': 'score desc',
    'hl': 'true',
    'hl.method': 'unified',
    'termVectors': 'true',
    'hl.fragsize': 10,
    'hl.snippets': 10,
    'hl.requireFieldMatch': 'true',
    'hl.fl': 'concept_name,concept_synonym_name,concept_mapped_name',
    'sow': 'false',
    'rows': 35,
    'wt': 'python',
})


for result in results.docs:
    print(f"{result} {results.highlighting.get(result['concept_id'])}" )

print(f"Saw {len(results.docs)} over {results.hits} results")
