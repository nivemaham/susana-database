/*********************************************************************************
# Copyright 2014 Observational Health Data Sciences and Informatics
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************

 ####### #     # ####### ######      #####  ######  #     #           #######
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######
 #     # #     # #     # #          #       #     # #     #    #    #       #
 #     # #     # #     # #          #     # #     # #     #     #  #  #     #
 ####### #     # ####### #           #####  ######  #     #      ##    #####


Script to load the common data model, version 5.0 vocabulary tables for PostgreSQL database on Windows (MS-DOS style file paths)
The database account running this script must have the "superuser" permission in the database.

Notes

1) There is no data file load for the SOURCE_TO_CONCEPT_MAP table because that table is deprecated in CDM version 5.0
2) This script assumes the CDM version 5 vocabulary zip file has been unzipped into the "../../athena/" directory.
3) If you unzipped your CDM version 5 vocabulary files into a different directory then replace all file paths below, with your directory path.
4) Truncate each table that will be lodaed below, before running this script.

last revised: 5 Dec 2014

author:  Lee Evans


*************************/
BEGIN;
 --SET CONSTRAINTS ALL DEFERRED;
TRUNCATE TABLE concept CASCADE;
TRUNCATE TABLE concept_class CASCADE;
TRUNCATE TABLE vocabulary CASCADE;
TRUNCATE TABLE domain CASCADE;
TRUNCATE TABLE relationship CASCADE;
TRUNCATE TABLE concept_synonym CASCADE;
TRUNCATE TABLE concept_ancestor CASCADE;
TRUNCATE TABLE concept_relationship CASCADE;
TRUNCATE TABLE drug_strength CASCADE;

\copy CONCEPT ( concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason) FROM 'private/athena/CONCEPT.csv' WITH DELIMITER E'|' CSV HEADER QUOTE E'"' ;
\copy CONCEPT_CLASS FROM 'private/athena/CONCEPT_CLASS.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy VOCABULARY FROM 'private/athena/VOCABULARY.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy DOMAIN FROM 'private/athena/DOMAIN.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy RELATIONSHIP FROM 'private/athena/RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy CONCEPT_SYNONYM FROM 'private/athena/CONCEPT_SYNONYM.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy CONCEPT_ANCESTOR FROM 'private/athena/CONCEPT_ANCESTOR.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy CONCEPT_RELATIONSHIP FROM 'private/athena/CONCEPT_RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
\copy DRUG_STRENGTH FROM 'private/athena/DRUG_STRENGTH.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
COMMIT;
