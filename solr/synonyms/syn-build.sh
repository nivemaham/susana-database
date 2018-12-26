#!/bin/bash
cat "solr/synonyms/synonyms-orig.txt" "solr/synonyms/synonyms-multi.txt" > "solr/configsets/omop/conf/synonyms.txt"
