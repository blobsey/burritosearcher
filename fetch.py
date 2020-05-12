import pickle, orjson

def fetch(docid):
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    filename = lookup[docid][1]
    with open(filename, "rb") as file:
        site = orjson.loads(file.read()) 
        
    with open(str(docid) + ".html", "w+") as dump:
        dump.write(site["content"])
        
print("docid: ")
docid = input()

fetch(int(docid))