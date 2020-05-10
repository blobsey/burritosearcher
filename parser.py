import os, time, re, gc, pickle, logging, glob, orjson, lxml
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor as multithreader

#GLOBAL VARIABLES IMPORTANT DONT DELETE
ps = PorterStemmer()
currentThread = 0
tempIndexes = dict() #4d dictionary bcuz i hate myself
reg = re.compile(r"[a-zA-Z]+")

#--PARAMETERS--
#global counter for what document were on
#might be a little off bcuz multithreading jank LOL
currentDoc = 0

#list to hold all the active threads
jobList = [] 

#number of threads to use
#might be broken but u could try higher numbers
numThreads = 4

#how many files to give each thread at once
chunkSize = 300

#folder containing corpus
corpusFolder = "corpus"

#folder to use as index
indexPath = "./index/" 

#nano wrote this and i love it
def tokenize(file):
    site = orjson.loads(file)
    soup = BeautifulSoup(site["content"], "lxml")
    tokens = re.findall(reg, ' '.join([line.lower() for line in soup.find_all(text=True)]))
    return [ps.stem(token) for token in tokens]

#TODO store an actual posting list instead of a 3D dict
def index(fileList, terms, docid):
    for filename in fileList: 
        with open(filename) as file:
            for token in tokenize(file.read()):
                label = token[0]
                terms[label].setdefault(token, defaultdict(int)) #if token doesnt exist, add it
                terms[label][token][docid] += 1  
                #print(terms[label][token][docid])
    logging.warning(" DONE with one thread") #debug statement
    
#takes a label (such as 'a' or 'b') and updates the corresponding index on disk           
def updateIndex(label, tempIndexes):
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
        
    
def dump(executor):
    global dumpNow, jobList
    futures.wait(jobList) #wait for all threads to finish
    #give each thread a label (such as 'a' 'b' etc) to work on
    for label in 'abcdefghijklmnopqrstuvwxyz':
        #updateIndex(label, tempIndexes)
        jobList.append(executor.submit(updateIndex, label, tempIndexes))
    futures.wait(jobList) #wait for all threads to finish
    for i in tempIndexes:
        for label in 'abcdefghijklmnopqrstuvwxyz':
            tempIndexes[i][label].clear() #empty the dictionary
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
    global dumpNow, jobList, tempIndexes, chunkSize
    currentDoc = 0
    
    #make a folder to hold all the partial indexes
    if not os.path.exists(indexPath):
        os.makedirs(indexPath)
    
    #each thread gets a dict
    for i in range(numThreads):
        tempIndexes[i] = dict()
        for label in 'abcdefghijklmnopqrstuvwxyz':
            tempIndexes[i][label] = dict()
        
    #make a bunch of empty pickles :\
    for label in 'abcdefghijklmnopqrstuvwxyz':
        f = open(indexPath + label + ".p", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    #executor is basically a manager for 4 threads
    with multithreader(max_workers=4) as executor:
        #for all files
        fileList = list()
        for filename in glob.glob(corpusFolder + "\**\*.json", recursive=True): 
            currentDoc += 1
            fileList.append(filename)
            #give each thread a file to work on
            if currentDoc % chunkSize == 0:
                jobList.append(executor.submit(index, fileList.copy(), tempIndexes[getNextThread()], currentDoc))
                fileList.clear()
            if currentDoc % (chunkSize*4) == 0:
                dump(executor)
            #index(fileList, tempIndexes[getThreadNumber()], currentDoc)
            
        #do one last iteration to get the last chunk
        jobList.append(executor.submit(index, fileList.copy(), tempIndexes[getNextThread()], currentDoc))
        dump(executor)
            



if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    currentDoc = 0
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("PRESS ANY KEY TO QUIT")
    input()
