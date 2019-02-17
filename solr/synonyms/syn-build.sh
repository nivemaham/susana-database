#!/bin/bash
cat "solr/synonyms/synonyms-orig.txt" "solr/synonyms/synonyms-multi.txt" "solr/synonyms/synonyms-mono.txt" > "solr/configsets/omop/conf/synonyms.txt"
cp solr/synonyms/synonyms-fr-en.txt solr/configsets/omop/conf/synonyms-fr-en.txt
echo "" > solr/configsets/omop/conf/synonyms-fr.txt
