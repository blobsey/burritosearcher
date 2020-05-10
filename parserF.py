import os, json, time, re, gc, pickle, logging
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor as multithreader

#GLOBAL VARIABLES IMPORTANT DONT DELETE
ps = PorterStemmer()
terms = dict()
#global counter for what document were on
#might be a little off bcuz multithreading jank LOL
currentDoc = 0
#flag for if we need to start dumping
dumpNow = False
#list to hold all the active threads
jobList = []

corpusFolder = "TEST"
indexPath = "./index/" 

#nano wrote this and i love it
def tokenize(file):
	reg = re.compile(r"[a-zA-Z]+")
	site = json.loads(file)
	soup = BeautifulSoup(site["content"],"html.parser")
	tokens = re.findall(reg, ' '.join([line.lower() for line in soup.find_all(text=True)]))
	return [ps.stem(token) for token in tokens]

#TODO store an actual posting list instead of a 3D dict
def index(file):
    global currentDoc
    docid = currentDoc
    for token in tokenize(file):
        label = token[0]
        terms.setdefault(label, dict()) #if label doesnt exist, add it 
        terms[label].setdefault(token, dict()) #if token doesnt exist, add it
        if docid in terms[label][token]: #if posting exists, increment
            terms[label][token][docid] += 1
        else: #if posting doesnt exist, add it
            terms[label][token][docid] = 1
    
#takes a label (such as 'a' or 'b') and updates the corresponding index on disk           
def updateIndex(label):
    logging.warning("START updating index %s", label) #debug statement
    #unpickle a certain index (file name will be "a.p" or "b.p" etc)
    with open(indexPath + str(label) + ".p", "rb") as f: 
        temp = pickle.load(f)
        temp.update(terms[label]) #use .update() to add new entries to dict
        
    #re-pickle the dict
    with open(indexPath + str(label) + ".p", "wb") as f:
        pickle.dump(temp, f, pickle.HIGHEST_PROTOCOL)
        
    logging.warning("STOP updating index %s", label) #debug statement
        
#takes a whole folder and indexes it
#probably should find a better way to divide workload
def indexFolder(root, directory, files):
    logging.warning("START indexing folder ...%s", root[-30:]) #debug statement
    global currentDoc, dumpNow 
    #index all files in a folder
    for filename in files:
        with open(os.path.join(root, filename)) as file: 
            currentDoc += 1
            index(file.read()) 
            if currentDoc % 1000 > 500: #every ~500 files, dump
                dumpNow = True
            #NOTE: its not exactly 1000 because it checks this while
            #other threads are running, and has to wait for them
            #to finish before it starts actually dumping...
    logging.warning("STOP indexing folder ...%s", root[-30:]) #debug statement
    
def dump(executor):
    global dumpNow, jobList
    futures.wait(jobList) #wait for all threads to finish
    #give each thread a label (such as 'a' 'b' etc) to work on
    for label in terms:
        updateIndex(label)
        #jobList.append(executor.submit(updateIndex, label))
    futures.wait(jobList) #wait for all threads to finish
    terms.clear() #empty the dictionary
    gc.collect() #garbage collection to save precious RAM
    dumpNow = False 
			
def main():
    global currentDoc, dumpNow, jobList
    #make a bunch of empty pickles :\
    for c in 'abcdefghijklmnopqrstuvwxyz':
        f = open(indexPath + c + ".p", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        

    #executor is basically a manager for 4 threads
    with multithreader(max_workers=4) as executor:
        #for all folders and subfolders
        for root, directory, files in os.walk(os.path.join(os.getcwd(), "./" + corpusFolder)):
            #give each thread a folder to work on
            indexFolder(root,directory,files)
            #jobList.append(executor.submit(indexFolder, root, directory, files))
            if dumpNow:
                dump(executor)
                
        #dump one last time before were done
        dump(executor)
            



if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    currentDoc = 0
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("PRESS ANY KEY TO QUIT")
    input()
