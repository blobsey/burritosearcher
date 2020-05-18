import pickle, orjson, lxml, sys
from bs4 import BeautifulSoup

def fetch(docid):
    with open("lookup.meme", "rb") as file:
        lookup = pickle.load(file)
    
    filename = lookup[docid][1]
    with open(filename, "rb") as file:
        site = orjson.loads(file.read().decode("utf-8", errors = "ignore"))

    soup = BeautifulSoup(site["content"], "lxml")
        
    with open(str(docid) + ".html", "w+", encoding="utf-8", errors="ignore") as dump:
        dump.write(str(soup))
        
##print("docid: ")
##docid = input()
##
##fetch(int(docid))
