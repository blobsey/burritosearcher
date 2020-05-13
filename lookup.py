import pickle

#helper function
#takes a docid in, returns the filename
#of the corresponding document
def lookup(docid):
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    return lookup[docid][1]
    
