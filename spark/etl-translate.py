
# ENGLISH TRANSLATION
# more there : http://www.nltk.org/howto/wordnet.html
#
url = "jdbc:postgresql://localhost:5432/mimic?user=mapper&currentSchema=map"
pg = sc._jvm.fr.aphp.eds.spark.postgres.PGUtil.apply(spark._jsparkSession, url, "/tmp/spark-postgres")

pg.inputBulk("select concept_id, concept_name from concept where vocabulary_id = 'CIM10'", False, 2, 1, "concept_id").registerTempTable("df")

from pyspark.sql.functions import udf

# Use udf to define a row-at-a-time udf
@udf('string')
# Input/output are both a single double value
def pandas_translate(v):
    #from nltk.tokenize import ToktokTokenizer
    from nltk.tokenize import ToktokTokenizer
    tok = ToktokTokenizer()
    words = tok.tokenize(v)
    return(translate(words))

def translate(words):
    from nltk.corpus import wordnet
    import re
    tmp = ''
    for word in words: 
        wordm = word
        if len(word) > 4:
            wordm = re.sub(r's$','',word)
            wordm = re.sub(r'ée?s?$','er',wordm)
        nets = wordnet.synsets(wordm, lang='fra')
        if len(nets) > 0:
            lem = nets[0].lemmas(lang="eng")
            if len(lem) > 0 :
                tmp += ' ' + str(lem[0].name())
            else:
                tmp += ' ' + wordm
        else:
            tmp += ' ' + wordm
    return(tmp)

df = sql("select * from df")
df.withColumn('concept_synonym_name', pandas_translate(df.concept_name)).registerTempTable("tr")
out = sql("select concept_id, concept_synonym_name, 4180186 as language_concept_id from tr")

pg.outputBulk("concept_synonym", out._jdf, 4)

pg.purgeTmp()
