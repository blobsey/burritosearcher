import os, time, re, gc, pickle, glob, orjson, lxml
import logging
import random
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as multithreader
import Posting

#GLOBAL VARIABLES IMPORTANT DONT DELETE
ps = PorterStemmer()
reg = re.compile(r"[a-zA-Z]+")
jobList = [] #list to hold all the active threads
log = logging.getLogger()
log.setLevel(0)

#--PARAMETERS--
#turn on/off multithreading for debug
goFast = True

#number of threads to use
numThreads = 6

#how many files to process at once
#each thread gets an ~equal section
chunkSize = 10000

#folder containing corpus
corpusFolder = "corpus"

#folder to use as index
indexPath = "./index/" 

#folder to use to store tempIndexes
#(they will be merged into partial indexes later)
tempIndexPath = "./temp/"

#only tokenize text in these tags
#DISABLED RIGHT NOW
tagList = ["h1", "h2", "h3", "strong", "b"]

#TODO store an actual posting list instead of a 3D dict
def index(fileList, tempIndex):
    #temp dict, will pickle dump later
    terms = dict() 
    for label in 'abcdefghijklmnopqrstuvwxyz': 
        terms[label] = dict()

    #fileList is tuples of (filesize, filepath, docid)
    for entry in fileList: 
        filename = entry[1]
        docid = entry[2]
        #open and tokenize json files contents
        with open(filename, "rb") as file:
            site = orjson.loads(file.read()) 
            soup = BeautifulSoup(site["content"], "lxml")
            words = defaultdict(int)
            for text in soup.find_all(text=True):
                for word in re.findall(reg, text.lower()):
                    words[word] += 1 #record term frequency
            for importantText in soup.find_all(tagList):
                for word in re.findall(reg, str(importantText.string).lower()):
                    words[word] += 4 #weight important terms higher

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
    global jobList, start_time
    futures.wait(jobList) #wait for all threads to finish
    log.warning("...took %s seconds", (time.time() - start_time))
    #give each thread a label (such as 'a' 'b' etc) to work on
    for label in 'abcdefghijklmnopqrstuvwxyz':
        if goFast:
            jobList.append(executor.submit(updateIndex, label))
        else:
            updateIndex(label)
    futures.wait(jobList) #wait for all threads to finish
    
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".temp", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    for i in range(numThreads):
        if goFast:
            jobList.append(executor.submit(gc.collect))

        
    futures.wait(jobList)
    
    gc.collect() #garbage collection to save precious RAM
 

#helper function
#breaks fileList into chunks of chunkSize
#files should be evenly distributed across the fileList
#in main, fileList is sorted biggest filesize -> smallest filesize
#so it should be a good mix of big and small files
def chunks(fileList):
    totalSize = len(fileList)
    for i in range(0, totalSize, chunkSize):
        yield fileList[i:i+chunkSize]
    
			
def main():
    global jobList, chunkSize, numThreads, start_time
    log.warning("Using %s threads", numThreads)
    log.warning("Processing chunks of %s files", chunkSize)
    #make a folder to hold all the partial indexes
    if not os.path.exists(indexPath):
        os.makedirs(indexPath)
    
    #make a folder to hold all temp indexes
    #(will be merged into partial indexes)
    if not os.path.exists(tempIndexPath):
        os.makedirs(tempIndexPath)
    
        
    #make a bunch of empty partial indexes
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
    with open("lookup.meme", "wb+") as f:
        pickle.dump(fileList, f, pickle.HIGHEST_PROTOCOL)
        
    fileList.sort(key=lambda s: s[0], reverse = False)
    log.warning("...took %s seconds", (time.time() - start_time))
    
    #executor is basically a manager for numThreads threads
    with multithreader(max_workers = numThreads) as executor:   
        #split fileList into chunks
        for chunk in chunks(fileList):
            #give each thread a section of chunk (even distr. of big, small files)
            start_time = time.time()
            log.warning("indexing filesizes %.5sKB to %.5sKB", chunk[0][0]/1024, chunk[-1][0]/1024)
            for i in range(numThreads):
                if goFast:
                    jobList.append(executor.submit(index, chunk[i::numThreads], i))
                else:
                    index(chunk[i::numThreads], i)
            dump(executor)

    
        





if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    start = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start))
    print("PRESS ANY KEY TO QUIT")
    input()
