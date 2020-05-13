import re, os, glob, pickle, orjson, bisect, time
from nltk.stem import PorterStemmer
from statistics import mean

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    start_time = time.time()
    
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    #load all partial indexes
    temp = dict()
    for label in "abcdefghijklmnopqrstuvwxyz":
        with open("./index/" + label + ".p", "rb") as file:
            temp[label] = pickle.load(file)
    
    while True:
        query = input("Input query search: ")
        start_time = time.time()
        
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
        results = list()    
        
        #for each token, store list of docs that contain
        for token in tokens: 
            
            postings = temp[token[0]].get(token) #get all docs with this token
            if postings != None: #if token exists in index
                results.append(set(postings.keys())) 
            else:
                results.append(set())
                
        print("fetching all docs for each token took", (time.time() - start_time), "seconds")
        start_time = time.time()
                    
        #rank by tfidf      
        rankings = list()
        for docid in set.intersection(*results): #boolean AND, intersect sets of docids
            tfidfMean = mean([temp[token[0]][token][docid].tfidf for token in tokens]) #avg tfidf score for all tokens
            bisect.insort(rankings, (tfidfMean, docid)) #record the avg tf-idf, docid

        for i in range(-1, -6, -1):
            with open(lookup[rankings[i][1]][1], "rb") as file: #look up the actual webpage from rankings[i][1] (the docid)
                site = orjson.loads(file.read())
                print ( site["url"].ljust(85), "tf-idf score:", "{:.3f}".format(rankings[i][0]))
                # with open(str(-i) + ".html", "wb+") as dump: #this code dumps the results into html files
                #     dump.write(site["content"].encode(encoding="utf-8", errors="ignore"))
        print("")


if __name__ == "__main__":
    run_query()
