import re, orjson, bisect, time, gc
try:
    import cPickle as pickle
except:
    import pickle
from nltk.stem import PorterStemmer
from statistics import mean

reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query(word = ""):
    start_time = time.time()
    
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
        
    temp = dict()
    
    for label in "abcdefghijklmnopqrstuvwxyz":
        with open("./index/" + label + ".positions", "rb") as file:
            temp[label] = pickle.load(file)
    
    

    while True:
        #query = input("Input query search: ")
        query = word
        start_time = time.time()
        
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]

        #for each token, store list of docs that contain
        results = list()  
        postings = dict()
        for token in tokens: 
            position = temp[token[0]].get(token)
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
            
                
        print("fetching all docs for each token took", (time.time() - start_time), "seconds")
        start_time = time.time()
                    
        #rank by tfidf      
        rankings = list()
        passed_list = list()
        for docid in set.intersection(*results): #boolean AND, intersect sets of docids
            tfidfMean = mean([postings[token][docid].tfidf for token in tokens]) #avg tfidf score for all tokens
            bisect.insort(rankings, (tfidfMean, docid)) #record the avg tf-idf, docid

        for i in range(-1, -6, -1):
            try:
                with open(lookup[rankings[i][1]][1], "rb") as file: #look up the actual webpage from rankings[i][1] (the docid)
                    site = orjson.loads(file.read())
                    passed_list.append((site["url"].ljust(85), "tf-idf score:", "{:.3f}".format(rankings[i][0])))
                    # with open(str(-i) + ".html", "wb+") as dump: #this code dumps the results into html files
                    #     dump.write(site["content"].encode(encoding="utf-8", errors="ignore"))
            except IndexError:
                    print ( "n\\a".ljust(85), "tf-idf score:", "{:.3f}".format(0))
        return passed_list


if __name__ == "__main__":
    run_query()
