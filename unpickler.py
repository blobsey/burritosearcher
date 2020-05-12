#for debugging
#opens a .p file and dumps contents to unpickle folder

import pickle, orjson, os

unpicklePath = "./unpickled/"

def fetch(docid):
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    filename = lookup[docid][1]
    with open(filename, "rb") as file:
        site = orjson.loads(file.read()) 
        
    with open(str(docid) + ".html", "w+") as dump:
        dump.write(site["content"])


def unpickle(label):
    with open("./index/" + label + ".p", "rb") as file:
        temp = pickle.load(file)
    
    if not os.path.exists(unpicklePath):
        os.makedirs(unpicklePath)
        
    with open(unpicklePath + label + ".txt", "w+") as dump:
        for token in temp:
            for docid in temp[token]:
                line = token + " docid:" + str(docid) + " word freq: " + str(temp[token][docid]) + "\n"
                dump.write(line)
               
for label in "abcdefghijklmnopqrstuvwxyz":
    unpickle(label)

        

