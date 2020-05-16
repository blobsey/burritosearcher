import re, orjson, bisect, gc
try:
    import cPickle as pickle
except:
    import pickle
from nltk.stem import PorterStemmer
from statistics import mean

class query:
    reg = re.compile(r"[a-zA-Z]+")
    ps = PorterStemmer()
    temp = dict()

    def __init__(self):
        for label in "abcdefghijklmnopqrstuvwxyz":
            with open("./index/" + label + ".positions", "rb") as file:
                self.temp[label] = pickle.load(file)

    def run_query(self, word = ""):
        with open("lookup.meme", "rb") as file:
            lookup = pickle.load(file)
        while True:
            #query = input("Input query search: ")
            query = word
            
            tokens = [self.ps.stem(word.lower()) for word in re.findall(self.reg, query)]

            #for each token, store list of docs that contain
            results = list()  
            postings = dict()
            for token in tokens: 
                position = self.temp[token[0]].get(token)
                if position != None: #if token exists in index
                    with open("./index/" + token[0] + ".p", "rb") as file:
                        file.seek(position)
                        postings[token] = pickle.load(file) #get all docs with this token
                    results.append(set(postings[token].keys())) 
                else:
                    results.append(set())
                    
                gc.collect()
                
            if not tokens:
                results.append(set())

                   
            #rank by tfidf      
            rankings = list()
            passed_list = list()
            for docid in set.intersection(*results): #boolean AND, intersect sets of docids
                tfidfSum = sum([postings[token][docid].tfidf for token in tokens]) #avg tfidf score for all tokens
                bisect.insort(rankings, (tfidfSum, docid)) #record the sum tf-idf, docid

            #pass results (strings) containing top5
            for i in range(-1, -6, -1):
                try:
                    with open(lookup[rankings[i][1]][1], "rb") as file: #look up the actual webpage from rankings[i][1] (the docid)
                        site = orjson.loads(file.read())
                        passed_list.append((site["url"], "\ntf-idf score: ", "{:.3f}".format(rankings[i][0]), "\n"))
##                        with open(str(-i) + ".html", "wb+") as dump: #this code dumself.ps the results into html files
##                            dump.write(site["content"].encode(encoding="utf-8", errors="ignore"))
                except IndexError:
                        passed_list.append( "n\\a" + "\ntf-idf score:" + "{:.3f}".format(0) + "\n")
            return passed_list



if __name__ == "__main__":
    meme = query()
    while(True):
        print("Enter search query: ", end="")
        inputMeme = input()
        memes = meme.run_query(inputMeme)
        for thing in memes:
            print (thing[0])
