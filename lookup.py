import pickle

def lookup(docid):
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    filename = lookup[docid][1]
    with open(filename, "rb") as fileList:
        return fileList[docid][1]