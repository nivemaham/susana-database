import pysolr

# create a connection to a solr server
#zookeeper = pysolr.ZooKeeper("localhost:9983")
#solr = pysolr.SolrCloud(zookeeper, "gettingstarted")
collection="omop-concept"
pysolr.ZooKeeper.CLUSTER_STATE = f'/collections/{collection}/state.json'
zookeeper = pysolr.ZooKeeper("localhost:9983")
solrCloud = pysolr.SolrCloud(zookeeper, collection, results_cls=dict)


# EDISMAX search
# handle multiword synonyms
# handle field query
# handle boosting
print("##\n# KEYWORD SEARCH\n##")
results = solrCloud.search('(child) AND standard_concept:S',  **{
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
    'rows': 2,
    'wt': 'python',
    'facet': 'true',
    'facet.field': ['domain_id','standard_concept'],
    'facet.limit': 5,
})

print(results)

for result in results["response"]["docs"]:
    print(f"{result['concept_id']} | {result['domain_id']} | {results['highlighting'].get(result['concept_id'])['concept_name'][0]}" )

for res in results["facet_counts"]["facet_fields"]:
    print(f"\nFACET {res}")
    for facet in results["facet_counts"]["facet_fields"][res]:
        print(f"{facet}")

print(f"Saw {len(results['response']['docs'])} over {results['response']['numFound']} results")
