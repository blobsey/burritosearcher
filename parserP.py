import os, time, re, gc, pickle, logging, glob, orjson, lxml
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as multithreader

#GLOBAL VARIABLES IMPORTANT DONT DELETE
ps = PorterStemmer()
currentThread = 0
tempIndexes = dict() #4d dictionary bcuz i hate myself
reg = re.compile(r"[a-zA-Z]+")
currentDoc = 0 #global counter for what document were on
jobList = [] #list to hold all the active threads

#--PARAMETERS--

#number of threads to use
#might be broken but u could try higher numbers
numThreads = 4

#how many files to give each thread at once
chunkSize = 300

#folder containing corpus
corpusFolder = "TEST2"

#folder to use as index
indexPath = "./indexP/" 

#folder to use to store tempIndexes
#(they will be merged into partial indexes later)
tempIndexPath = "./temp/"


#nano wrote this and i love it <3
def tokenize(file):
    site = orjson.loads(file)
    soup = BeautifulSoup(site["content"], "lxml")
    tokens = re.findall(reg, ' '.join([line.lower() for line in soup.find_all(text=True)]))
    return [ps.stem(token) for token in tokens]

#TODO store an actual posting list instead of a 3D dict
def index(fileList, docid, tempIndex):
    #temp dict, will pickle dump later
    terms = dict() 
    for label in 'abcdefghijklmnopqrstuvwxyz': 
        terms[label] = dict()

    for filename in fileList: 
        with open(filename) as file:
            for token in tokenize(file.read()):
                label = token[0]
                terms[label].setdefault(token, defaultdict(int)) #if token doesnt exist, add it
                terms[label][token][docid] += 1  
                
    updateTempIndex(tempIndex, terms) #write to temp file to be merged later
    print(" DONE with one thread") #debug statement
    
def updateTempIndex(tempIndex, terms):
    with open(tempIndexPath + str(tempIndex) + ".temp", "wb") as f:
        pickle.dump(terms, f, pickle.HIGHEST_PROTOCOL)

#takes a label (such as 'a' or 'b') and updates the corresponding index on disk           
def updateIndex(label):
    fileObjects = []
    tempIndexes = dict()
    #open all temp indexes to merge them
    for i in range(numThreads):
        fileObjects.append(open(tempIndexPath + str(i) + ".temp", "rb"))
            
    #unpickle them               
    for i,f in enumerate(fileObjects):
        tempIndexes[i] = pickle.load(f)
        
    # logging.warning("START updating index %s", label) #debug statement
    #unpickle a certain index (file name will be "a.p" or "b.p" etc)
    with open(indexPath + str(label) + ".p", "rb") as f: 
        temp = pickle.load(f)
        for i in tempIndexes:
            if label in tempIndexes[i]:
                temp.update(tempIndexes[i][label]) #use .update() to add new entries to dict
        
    #re-pickle the dict
    with open(indexPath + str(label) + ".p", "wb") as f:
        pickle.dump(temp, f, pickle.HIGHEST_PROTOCOL)
    
    #always remember to close file handles :)
    for f in fileObjects:
        f.close()
        
    
def dump(executor):
    global jobList
    futures.wait(jobList) #wait for all threads to finish
    #give each thread a label (such as 'a' 'b' etc) to work on
    for label in 'abcdefghijklmnopqrstuvwxyz':
        #updateIndex(label)
        jobList.append(executor.submit(updateIndex, label))
    futures.wait(jobList) #wait for all threads to finish
    
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".temp", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
    
    gc.collect() #garbage collection to save precious RAM
    logging.warning(" UPDATING Indexes") #debug statement
    
def getNextThread():
    global currentThread, numThreads
    logging.warning(" JOB created for thread %s", currentThread) #debug statement
    temp = currentThread
    currentThread += 1
    if currentThread >= numThreads:
        currentThread = 0
    return temp
    
			
def main():
    global dumpNow, jobList, tempIndexes, chunkSize, numThreads
    currentDoc = 0
    
    #make a folder to hold all the partial indexes
    if not os.path.exists(indexPath):
        os.makedirs(indexPath)
    
    #make a folder to hold all temp indexes
    #(will be merged into partial indexes)
    if not os.path.exists(tempIndexPath):
        os.makedirs(tempIndexPath)
    
        
    #make a bunch of empty pickles :\
    for label in 'abcdefghijklmnopqrstuvwxyz':
        f = open(indexPath + label + ".p", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    #make a bunch of empty temp indexes
    #each thread gets one to do its own indexing
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".temp", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    #executor is basically a manager for 4 threads
    with multithreader(max_workers=4) as executor:
        #build a list of all files
        fileList = list()
        for filename in glob.glob(corpusFolder + "\**\*.json", recursive=True): 
            currentDoc += 1
            fileList.append(filename)
            #give each thread a chunk of files to work on
            if currentDoc % chunkSize == 0:
                #index(fileList.copy(), currentDoc, getNextThread())
                jobList.append(executor.submit(index, fileList.copy(), currentDoc, getNextThread()))
                fileList.clear()
            if currentDoc % (chunkSize*numThreads) == 0:
                dump(executor)
            
        #do one last iteration to get the last chunk
        #index(fileList.copy(), currentDoc, getNextThread())
        jobList.append(executor.submit(index, fileList.copy(), currentDoc, getNextThread()))
        dump(executor)
            



if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    currentDoc = 0
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("PRESS ANY KEY TO QUIT")
    input()
