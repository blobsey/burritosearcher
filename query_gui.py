from lookup import lookup
from unpickler import unpickle
from nltk.stem import PorterStemmer

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    while True:
        print("Input query search: ")
        query = input()
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
        for token in tokens:
            unpickle(token[0])
        #get info from dump
        #rank
        #sort on ranking
        '''results = ["page1", "page2"]
        println([i for i in results]])'''
        #Find documents
        #clear the dump file (has all unpickled information!)

# temp[token][docid].termfreq = integer 
# temp[token][docid].tfidf = float 

if __name__ == "__main__":
    run_query()