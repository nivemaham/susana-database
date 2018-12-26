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
for (id, hl) in results.highlighting.items():
    print(f"{id} : {hl}")

print(f"Saw {len(results.docs)} over {results.hits} results")


#
# see there http://lucene.472066.n3.nabble.com/Using-MoreLikeThisHandler-td532582.html
# how to config that stuff

print("##\n# SIMILAR SEARCH\n##")
# yet possible to:
# search for standard concepts
# filter by domain
# modify boosts on the fly
solr = pysolr.Solr('http://localhost:8983/solr/omop-concept',search_handler='/mlt')
results = solr.search('(concept_id:36210200)',  **{
    "mlt.fl":"concept_synonym_name,concept_name,vocabulary_id",
    "mlt.mindf":"1",
    "mlt.mintf":"1",
    "rows": 15,
    "mlt.boost": "true",
    "mlt.interestingTerms": "details",
    "mlt.qf": "concept_name^1.5 concept_synonym_name^0.5"
})

for (key,value) in results.raw_response.items():
    if key=="interestingTerms":
        print(f'Weighted query: {value}')

for result in results.docs:
    if 'standard_concept' not in result:
        standard_concept = None
    else:
        standard_concept = result['standard_concept']
    print(f"{result['concept_id']} : {result['concept_name']} ({result['domain_id']} - {standard_concept})")

print(f"Saw {len(results.docs)} over {results.hits} results")
