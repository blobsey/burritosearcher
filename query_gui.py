import re, os, glob, pickle, orjson
from nltk.stem import PorterStemmer
#from unpickler import unpickle
from lookup import lookup
from statistics import mean
reg = re.compile(r"[a-zA-Z]+")
ps = PorterStemmer()

def run_query():
    #open all the pickles >:))
    temp = dict()
    for label in "abcdefghijklmnopqrstuvwxyz":
        with open("./index/" + label + ".p", "rb") as file:
            temp[label] = pickle.load(file)
    
    while True:
        query = input("Input query search: ")
        
        tokens = [ps.stem(word.lower()) for word in re.findall(reg, query)]
        results = list()    
        #for each token, store list of docs that contain
        for token in tokens: 
            
            postings = temp[token[0]].get(token) #get all docs with this token
            if postings != None: #if token exists in index
                results.append(set(postings.keys())) 
            else:
                results.append(set())
                    
        #rank by tfidf      
        rankings = list()
        for docid in set.intersection(*results): #boolean AND, intersect sets of docids
            tfidfSum = 0
            for token in tokens:
                tfidfSum += temp[token[0]][token][docid].tfidf
            rankings.append((tfidfSum/len(tokens), docid))#record the avg tf-idf 
        rankings = sorted(rankings, reverse=True)
        for i in range(5):
            with open(lookup(rankings[i][1]), "rb") as file:
                site = orjson.loads(file.read())
                print ( site["url"], "tf-idf score:", rankings[i][0])
        


if __name__ == "__main__":
    run_query()
