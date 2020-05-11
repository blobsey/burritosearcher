import os, time, re, gc, pickle, glob, orjson, lxml
import logging
import random
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as multithreader

#GLOBAL VARIABLES IMPORTANT DONT DELETE
ps = PorterStemmer()
reg = re.compile(r"[a-zA-Z]+")
jobList = [] #list to hold all the active threads
log = logging.getLogger()
log.setLevel(0)

#--PARAMETERS--

#number of threads to use
#might be broken but u could try higher numbers
numThreads = 6

#how many files to process at once
#each thread gets an ~equal section
chunkSize = 1600

#folder containing corpus
corpusFolder = "corpus"

#folder to use as index
indexPath = "./indexP/" 

#folder to use to store tempIndexes
#(they will be merged into partial indexes later)
tempIndexPath = "./temp/"

#only tokenize text in these tags
tagList = ["h1", "h2", "h3", "p", "b"]

#TODO store an actual posting list instead of a 3D dict
def index(fileList, tempIndex):
    #temp dict, will pickle dump later
    start_time = time.time()
    
    terms = dict() 
    for label in 'abcdefghijklmnopqrstuvwxyz': 
        terms[label] = dict()

    #fileList is tuples of (filesize, filepath, docid)
    for entry in fileList: 
        filename = entry[1]
        docid = entry[2]
        with open(filename, "rb") as file:
            
            #open and tokenize json files contents
            site = orjson.loads(file.read()) 
            soup = BeautifulSoup(site["content"], "lxml")
            words = defaultdict(int)
            for tag in soup.find_all(text=True): 
                #if tag.name in tagList:
                    for word in re.findall(reg, str(tag).lower()):
                        #holds term frequency
                        words[word] += 1

            #stem tokens and add to index
            for word in words:
                label = word[0]
                token = ps.stem(word)
                terms[label].setdefault(token, defaultdict(int)) #if token doesnt exist, add it
                terms[label][token][docid] += words[word]
       
    #dump everything to temp file to merge later
    with open(tempIndexPath + str(tempIndex) + ".temp", "wb") as f:
        pickle.dump(terms, f, pickle.HIGHEST_PROTOCOL)         
    gc.collect()

    # log.warning("indexed from: " +  fileList[0][7:]) #debug statement
    # log.warning("          to: " +  fileList[-1][7:]) #debug statement
    log.warning("thread %s took %s seconds", tempIndex, (time.time() - start_time))
    
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
        updateIndex(label)
        #jobList.append(executor.submit(updateIndex, label))
    futures.wait(jobList) #wait for all threads to finish
    
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".temp", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    for i in range(numThreads):
        jobList.append(executor.submit(gc.collect))
    futures.wait(jobList)
    
    gc.collect() #garbage collection to save precious RAM
    log.warning("Partial Indexes updated...") #debug statement
 

#helper function
#breaks fileList into chunks of chunkSize
#files should be evenly distributed across the fileList
#in main, fileList is sorted biggest filesize -> smallest filesize
#so it should be a good mix of big and small files
def chunks(fileList):
    totalSize = len(fileList)
    filesPerChunk = int(totalSize/chunkSize)
    if chunkSize > totalSize:
        filesPerChunk = 1
    for i in range(filesPerChunk):
        yield fileList[i::filesPerChunk]
    
			
def main():
    global dumpNow, jobList, tempIndexes, chunkSize, numThreads
    
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
        
    log.warning("building file list...")
    start_time = time.time()
    #build a list of all files
    fileList = list()
    for docid, filename in enumerate(glob.glob(corpusFolder + "\**\*.json", recursive=True)): 
        fileList.append( (os.path.getsize (filename), filename, docid) )
      
    #sort filelist by file size
    #makes threads finish at similar times
    fileList.sort(key=lambda s: s[0], reverse = True)
    log.warning("...took %s seconds", (time.time() - start_time))
    
    #executor is basically a manager for numThreads threads
    with multithreader(max_workers = numThreads) as executor:   
        #split fileList into chunks
        #chunk = even distribution of small, medium, big files
        for chunk in chunks(fileList):
            #give each thread a section of chunk
            #section = even distribution of small, medium, big files
            for i in range(numThreads):
                #index(chunk[i::numThreads], i)
                jobList.append(executor.submit(index, chunk[i::numThreads], i))
            dump(executor)





if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    currentDoc = 0
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
    print("PRESS ANY KEY TO QUIT")
    input()
