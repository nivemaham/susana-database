import logging
from pysolrwrapper.core import SolrQuery
from pysolrwrapper.filter import (FilterText, FilterColumnEnum, FilterColumnNum, AND, OR, NOT)
from pysolrwrapper.utils import *

#
# This algo uses both numerics avg and frequency
#
def solr_algo_numerics(value_avg, frequency):
    dic = {
            'f.fr.qf': 'concept_name_fr^1 concept_synonym_name_fr^1 concept_mapped_name_fr^1',
            'f.en.qf': 'concept_name_en^1 concept_synonym_name_en^1 concept_mapped_name_en^1',
            'f.fr_en.qf': 'concept_name_fr_en^1 concept_synonym_name_fr_en^1 concept_mapped_name_fr_en^1',
            'boost': f'sum(sub(1,abs(sub(value_avg,{value_avg}))),pow(sub(frequency,{frequency}),2))'
            }
    return dic

#
# This algo uses frequency boost
#
def solr_algo_frequency(frequency):
    dic = {
            'f.fr.qf': 'concept_name_fr^1 concept_synonym_name_fr^1 concept_mapped_name_fr^1',
            'f.en.qf': 'concept_name_en^1 concept_synonym_name_en^1 concept_mapped_name_en^1',
            'f.fr_en.qf': 'concept_name_fr_en^1 concept_synonym_name_fr_en^1 concept_mapped_name_fr_en^1',
            'boost': f'pow(sub(frequency,{frequency}),2)'
            }
    return dic

#
# This algo uses frequency value
#
def solr_algo_value(value_avg):
    dic = {
            'f.fr.qf': 'concept_name_fr^1 concept_synonym_name_fr^1 concept_mapped_name_fr^1',
            'f.en.qf': 'concept_name_en^1 concept_synonym_name_en^1 concept_mapped_name_en^1',
            'f.fr_en.qf': 'concept_name_fr_en^1 concept_synonym_name_fr_en^1 concept_mapped_name_fr_en^1',
            'boost': f'sub(1,abs(sub(value_avg,{value_avg})))'
            }
    return dic

# This results in [fr->en]:
# 1.  f.fr.qf=concept_name_fr concept_synonym_name_fr concept_mapped_name_fr concept_hierarchy_name_fr
# 2. &f.tr_en.qf=concept_name_en concept_synonym_name_en concept_mapped_name_en concept_hierarchy_name_en
# 3. &f.fr_en.qf=concept_name_fr_en concept_synonym_name_fr_en concept_mapped_name_fr_en concept_hierarchy_name_fr_en
# query: fr:(bonjour monde) tr_en:(hello world) fr_en:(bonjour monde)
def solr_algo_complex(value_avg):
    dic = {
            'f.fr.qf': 'concept_name_fr^1 concept_synonym_name_fr^1 concept_mapped_name_fr^1',
            'f.en.qf': 'concept_name_en^1 concept_synonym_name_en^1 concept_mapped_name_en^1',
            'f.fr_en.qf': 'concept_name_fr_en^1 concept_synonym_name_fr_en^1 concept_mapped_name_fr_en^1',
            'boost': f'sub(1,abs(sub(value_avg,{value_avg})))'
            }
    return dic

def solr_algo_simple():
    dic = {
            'f.fr.qf': 'concept_name_fr^1 concept_synonym_name_fr^1 concept_mapped_name_fr^1',
            'f.en.qf': 'concept_name_en^1 concept_synonym_name_en^1 concept_mapped_name_en^1',
            'f.fr_en.qf': 'concept_name_fr_en^1 concept_synonym_name_fr_en^1 concept_mapped_name_fr_en^1'
            }
    return dic

def solr_query_multi(en=None, fr=None, fr_en=None):
    res = []
    if en:
        res.append(str(FilterColumnEnum("en",[en])))
    if fr:
        res.append(str(FilterColumnEnum("fr",[fr])))
    if fr_en:
        res.append(str(FilterColumnEnum("fr_en",[fr_en])))
    return [" OR ".join(res)]



def test_conn():
    solr_query = SolrQuery("localhost:9983", "omop-concept")\
        .set_type_edismax()\
        .select(["id","value_avg","concept_name","score"])\
        .query(solr_query_multi(fr="mucolipidosis ii [cell-embedded disease] [i.cell disease]",en="mucolipidosis ii [cell-embedded disease] [i.cell disease]",fr_en="mucolipidosis ii [cell-embedded disease] [i.cell disease]"))\
        .algo(solr_algo_simple())\
        .highlight(["concept_name"])\
        .facet(10, ["standard_concept", "is_mapped"])\
        .sort("score", "desc")\
        .page(1)\
        .limit(10)
    logging.warning(solr_query.run())

test_conn()
