#!/bin/bash
sources="
APHP
SNDS
BDX
LILLE
TLS
"
for PROJECT in $sources
do
QUERY=$(cat <<EOF 
\\copy (
select 
 concept_id        
, concept_name     
, domain_id        
, vocabulary_id    
, concept_class_id 
, standard_concept 
, concept_code     
, valid_start_date 
, valid_end_date   
, invalid_reason   
, m_language_id    
, m_project_id     
from concept 
where TRUE 
AND m_project_id IN ( 
	select m_project_id 
	from mapper_project 
	where lower(m_project_type_id) = lower('$PROJECT')
)) TO 'MAPPER-${PROJECT}-concept.csv' CSV HEADER;
EOF
)
echo $QUERY
psql "host=localhost dbname=mimic user=mapper options=--search_path=map" -c "$QUERY"

QUERY=$(cat <<EOF 
\\copy (
select 
  s.concept_id             
, s.concept_synonym_name   
, s.language_concept_id    
from concept_synonym  s
join concept using (concept_id)
where TRUE 
AND m_project_id IN ( 
	select m_project_id 
	from mapper_project 
	where lower(m_project_type_id) = lower('$PROJECT')
)) TO 'MAPPER-${PROJECT}-concept_synonym.csv' CSV HEADER;
EOF
)
echo $QUERY
psql "host=localhost dbname=mimic user=mapper options=--search_path=map" -c "$QUERY"


QUERY=$(cat <<EOF 
\\copy (
select 
  r.concept_id_1    
, r.concept_id_2    
, r.relationship_id 
, r.valid_start_date
, r.valid_end_date  
, r.invalid_reason  
from concept_relationship r
join concept c on (c.concept_id = r.concept_id_1)
where TRUE 
AND m_project_id IN ( 
	select m_project_id 
	from mapper_project 
	where lower(m_project_type_id) = lower('$PROJECT')
)) TO 'MAPPER-${PROJECT}-concept_relationship.csv' CSV HEADER;
EOF
)
echo $QUERY
psql "host=localhost dbname=mimic user=mapper options=--search_path=map" -c "$QUERY"

# pivot table
QUERY=$(cat <<EOF 
\\copy (
with
cr as (
	select
	  m_concept_relationship_id
	, concept_id_1
	, concept_id_2
	from (SELECT 
		  m_concept_relationship_id
		, concept_id_1
		, concept_id_2
		, max(concept_id_2) over(partition by concept_id_1) as max_concept_id_2
		FROM concept_relationship
		WHERE true
		AND relationship_id = 'Maps to' 
		AND m_modif_end_datetime is null
		AND invalid_reason is null) as tmp
	where concept_id_2 = concept_id_2
),
vote as (
	SELECT 
	  m_concept_id as m_concept_relationship_id
	, sum(case when m_value_as_string ='UP' then 1 else -1 end) over(partition by m_concept_id) vote_value
	, count(1) over(partition by m_concept_id) vote_count
	FROM mapper_statistic 
	WHERE m_statistic_type_id = 'VOTE_MAPPING' 
)
SELECT 
  c.concept_id concept_id_l
, case when c.standard_concept = 'S' THEN c.concept_id ELSE coalesce(cs.concept_id, 0) END concept_id_s    
, c.concept_code concept_code_l
, case when c.standard_concept = 'S' THEN c.concept_code ELSE coalesce(cs.concept_code, 'No matching concept') END concept_code_s    
, c.concept_name concept_name_l
, case when c.standard_concept = 'S' THEN c.concept_name ELSE coalesce(cs.concept_name, 'No matching concept') END concept_name_s    
, c.domain_id domain_id_l
, case when c.standard_concept = 'S' THEN c.domain_id ELSE coalesce(cs.domain_id, 'Metadata') END domain_id_s    
, c.vocabulary_id vocabulary_id_l
, case when c.standard_concept = 'S' THEN c.vocabulary_id ELSE coalesce(cs.vocabulary_id, 'None') END vocabulary_id_s    
, vote_count
, vote_value
FROM concept c
LEFT JOIN cr on c.concept_id = cr.concept_id_1
LEFT JOIN concept cs on cr.concept_id_2 = cs.concept_id
LEFT JOIN vote on vote.m_concept_relationship_id = cr.m_concept_relationship_id
WHERE TRUE 
AND c.m_project_id IN ( 
	select m_project_id 
	from mapper_project 
	where lower(m_project_type_id) = lower('$PROJECT')
) order by concept_id_s desc) TO 'MAPPER-${PROJECT}-concept_pivot.csv' CSV HEADER;
EOF
)
echo $QUERY
psql "host=localhost dbname=mimic user=mapper options=--search_path=map" -c "$QUERY"

done
#    del as (
#      delete
#      from concept 
#      where vocabulary_id = '$vocabulary_id' 
#      AND domain_id = '$domain_id'
#      AND m_project_id IN (select m_project_id from mapper_project where lower(m_project_type_id) = lower('$m_project_type_id'))
#      returning concept_id
#    ),
#    del2 AS (
#      delete from mapper_statistic where m_concept_id in (select concept_id from del)
#    ),
#    del3 AS (
#      delete from concept_synonym where concept_id in (select concept_id from del)
#    )
#    delete from concept_relationship where concept_id_1 in (select concept_id from del) OR concept_id_2 in (select concept_id from del)
