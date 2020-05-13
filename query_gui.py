import re, os, glob
from nltk.stem import PorterStemmer
from unpickler import unpickle

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    while True:
        results = []
        print("Input query search: ")
        query = input()
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
        for token in tokens:
            unpickle(token[0])
            #get info from dump and rank
            with open("./unpickled/" + token[0] + ".txt", "r+") as dump:
                #TODO: Ranking and insert into results
                pass
        println([i for i in results]])

        #Clear the unpickled files
        for file in glob.glob('./unpickled/*.txt'):
            try:
                file.unlink()
            except OSError as e:
                print("Error: %s : %s" % (file, e.strerror))

# temp[token][docid].termfreq = integer 
# temp[token][docid].tfidf = float 

if __name__ == "__main__":
    run_query()