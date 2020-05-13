import re, os, glob, pickle
from nltk.stem import PorterStemmer
#from unpickler import unpickle
from lookup import lookup

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    while True:
        results = []
        query = input("Input query search: ")
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
        for token in tokens:
            #unpickle(token[0])
            #get info from dump and rank
            with open("./index/" + token[0] + ".p", "rb") as dump:
                #TODO: Ranking and insert into results
                temp = pickle.load(dump)
                for line in temp:
                    #prints word
                    print(temp)
        println([i for i in results])

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
