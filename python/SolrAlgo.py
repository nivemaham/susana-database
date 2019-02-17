
#
# This algo uses both numerics avg and frequency
#
def solr_algo_numerics(value_avg, frequency):
    dic = {
            'qf': 'concept_name^1 concept_synonym_name^1 concept_mapped_name^1',
            'boost': f'sum(sub(1,abs(sub(value_avg,{value_avg}))),pow(sub(frequency,{frequency}),2))'
            }
    return dic

#
# This algo uses frequency boost
#
def solr_algo_frequency(frequency):
    dic = {
            'qf': 'concept_name^1 concept_synonym_name^1 concept_mapped_name^1',
            'boost': f'pow(sub(frequency,{frequency}),2)'
            }
    return dic

#
# This algo uses frequency value
#
def solr_algo_value(value_avg):
    dic = {
            'qf': 'concept_name^1 concept_synonym_name^1 concept_mapped_name^1',
            'boost': f'sub(1,abs(sub(value_avg,{value_avg})))'
            }
    return dic


import logging
from pysolrwrapper.core import SolrQuery
from pysolrwrapper.filter import (FilterText, FilterColumnEnum, FilterColumnNum, AND, OR, NOT)
from pysolrwrapper.utils import *


def test_conn():
    solr_query = SolrQuery("localhost:9983", "omop-concept")\
        .set_type_edismax()\
        .highlight(["concept_name"])\
        .add_filter(FilterText( ["synovial"]))\
        .add_where(FilterColumnEnum("value_is_numeric","true"))\
        .facet(10, ["standard_concept", "is_mapped"])\
        .select(["id","value_avg","concept_name"])\
        .algo(solr_algo_numerics(21000,4))\
        .sort("score", "desc")\
        .page(1)\
        .limit(10)
    logging.warning(solr_query.run())

test_conn()
