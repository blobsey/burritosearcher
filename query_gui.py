import re, pickle, orjson, bisect, time, gc
from nltk.stem import PorterStemmer
from statistics import mean

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    start_time = time.time()
    
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    temp = dict() #holds the partial indexes
    unloadQueue = list() #so we can unload oldest index
    print("Loading", end="", flush=True)
    for label in "clmaose": #dont worry about it :) 
        with open("./index/" + label + ".p", "rb") as file:
            temp[label] = pickle.load(file)
        print(".", end="", flush=True)
        unloadQueue.append(label)
    print("")

    while True:
        query = input("Input query search: ")
        start_time = time.time()
        
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
          
        loadMe = list()
        for token in tokens:
            if token[0] not in unloadQueue:
                loadMe.append(token[0])
            else: #if in cache, re-add it to the front
                unloadQueue.remove(token[0])

        #for each token, store list of docs that contain
        results = list()  
        for token in tokens: 
            unloadNow = False
            if token[0] in loadMe: #if token not in cache
                unloadNow = True
                with open("./index/" + token[0] + ".p", "rb") as file:
                    temp[token[0]] = pickle.load(file)
            unloadQueue.append(token[0])
            
            postings = temp[token[0]].get(token) #get all docs with this token
            if postings != None: #if token exists in index
                results.append(set(postings.keys())) 
            else:
                results.append(set())
                
            if unloadNow: #we added a new partialindex to cache, so delete the oldest one
                del temp[unloadQueue.pop(0)]
                gc.collect()
            
                
        print("fetching all docs for each token took", (time.time() - start_time), "seconds")
        start_time = time.time()
                    
        #rank by tfidf      
        rankings = list()
        for docid in set.intersection(*results): #boolean AND, intersect sets of docids
            tfidfMean = mean([temp[token[0]][token][docid].tfidf for token in tokens]) #avg tfidf score for all tokens
            bisect.insort(rankings, (tfidfMean, docid)) #record the avg tf-idf, docid

        for i in range(-1, -6, -1):
            try:
                with open(lookup[rankings[i][1]][1], "rb") as file: #look up the actual webpage from rankings[i][1] (the docid)
                    site = orjson.loads(file.read())
                    print ( site["url"].ljust(85), "tf-idf score:", "{:.3f}".format(rankings[i][0]))
                    # with open(str(-i) + ".html", "wb+") as dump: #this code dumps the results into html files
                    #     dump.write(site["content"].encode(encoding="utf-8", errors="ignore"))
            except IndexError:
                    print ( "n\\a".ljust(85), "tf-idf score:", "{:.3f}".format(0))
        print("")


if __name__ == "__main__":
    run_query()
