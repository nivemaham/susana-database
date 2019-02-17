import re
from nltk.corpus import wordnet as wn

# ENGLISH TRANSLATION
# more there : http://www.nltk.org/howto/wordnet.html
#

with open("solr/synonyms/synonyms-fr-en.txt", "r+") as myfile:
    for word in wn.all_lemma_names(lang="fra"):
            nets = wn.synsets(word, lang='eng')
            wordname = re.sub(r'_',' ',word)
            if len(nets) > 0:
                tmp = []
                for net in nets:
                    ls = net.lemmas(lang="eng")
                    for l in ls:
                        lname = re.sub(r'_',' ',l.name())
                        tmp.append(lname)
                if wordname.strip() !='':
                    myfile.write(wordname + " => " + ','.join(tmp) + "\n")



