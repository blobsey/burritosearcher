#for debugging
#opens a .p file and dumps contents to unpickle folder

import pickle, orjson, os
from posting import Posting
from collections import defaultdict

unpicklePath = "./unpickled/"

def dd():
    return defaultdict(dd2)

def dd2():
    return defaultdict(Posting)
        

def unpickle(label):
    with open("./index/" + label + ".p", "rb") as file:
        temp = pickle.load(file)
    
    if not os.path.exists(unpicklePath):
        os.makedirs(unpicklePath)
        
    with open(unpicklePath + label + ".txt", "w+") as dump:
        for token in temp:
            for docid in temp[token]:
                line = token + " \tdocid:" + str(docid) + " \tterm freq: " + str(temp[token][docid].termfreq) + " \ttfidf: " + str(temp[token][docid].tfidf) + "\n"
                dump.write(line)
               
for label in "abcdefghijklmnopqrstuvwxyz":
    unpickle(label)

        

