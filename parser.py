import os, time, re, gc, pickle, glob, orjson, lxml, math
import logging
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as multithreader
from posting import Posting

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
tagList = ["h1", "h2", "h3", "strong", "b"]
             

def index(fileList, tempIndex):
    #temp dict, will pickle dump later
    #yes i know its ugly
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
            totalWords = 0
            for text in soup.find_all(text=True):
                for word in re.findall(reg, text.lower()):
                    words[word] += 1 #record term frequency
                    totalWords += 1 #keep track of total words in doc
            for importantText in soup.find_all(tagList):
                for word in re.findall(reg, str(importantText.string).lower()):
                    words[word] += 4 #weight important terms higher

            #stem tokens and add to index
            #also add dummy value, will be filled by tf-idf score later
            for word in words:
                label = word[0]
                token = ps.stem(word)
                terms[label].setdefault(token, defaultdict(Posting))
                terms[label][token][docid].updateFreq(words[word]/totalWords)
            
       
    #dump everything to temp file to merge later
    with open(tempIndexPath + str(tempIndex) + ".temp", "wb") as f:
        pickle.dump(terms, f, pickle.HIGHEST_PROTOCOL)         
    gc.collect()


    
#takes a label (such as 'a' or 'b') and updates the corresponding index on disk           
def updateIndex(labels):
    #unpickle a partialIndex (file name will be "a.p" or "b.p" etc)
    #then unpickle each tempIndex from temp folder and dump to partialIndex
    for label in labels:
        with open(indexPath + str(label) + ".p", "rb") as f: #open partialIndex
            partialIndex = pickle.load(f)
            for i in range(numThreads): #open each of the tempIndexes
                with open(tempIndexPath + str(i) + ".temp", "rb") as temp:
                    tempIndex = pickle.load(temp)
                    for token in tempIndex[label]:
                        for docid in tempIndex[label][token]:
                            partialIndex.setdefault(token, defaultdict(Posting))
                            partialIndex[token][docid].updateFreq(tempIndex[label][token][docid].termfreq)
                del tempIndex
                gc.collect()
        #re-pickle the dict
        with open(indexPath + str(label) + ".p", "wb") as f:
            pickle.dump(partialIndex, f, pickle.HIGHEST_PROTOCOL)
        gc.collect() 
    
def dump(executor):
    global jobList, start_time
    start_time = time.time()
    #give each thread a label (such as 'a' 'b' etc) to work on
    for labels in labelChunks():
        if goFast:
            jobList.append(executor.submit(updateIndex, labels))
        else:
            updateIndex(labels)
    futures.wait(jobList) #wait for all threads to finish

    #clear temp indexes for the next chunk of data
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".temp", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()

    #might be useless, calls garbage collection on each of the threads
    for i in range(numThreads):
        if goFast:
            jobList.append(executor.submit(gc.collect))

        
    futures.wait(jobList)
    
    log.warning("Dumping took %s seconds", (time.time() - start_time))
    
    gc.collect() #garbage collection to save precious RAM
 

#helper function
#breaks fileList into chunks of chunkSize
#files in chunks should be evenly distributed across the fileList
#in main, fileList is sorted biggest filesize -> smallest filesize
#so it should be a good mix of big and small files
def chunks(fileList):
    totalSize = len(fileList)
    for i in range(0, totalSize, chunkSize):
        yield fileList[i:i+chunkSize]
        
def labelChunks():
    alphabet = "abcdefghijklmnopqrstuvwxyz"
    for i in range(numThreads):
        yield alphabet[i::numThreads]
        
def calculateTFIDF(labels, N):
    for label in labels:
        with open(indexPath + label + ".p", "rb") as file:
            partialIndex = pickle.load(file)
            
        for token in partialIndex:
            for docid in partialIndex[token]:
                tf = partialIndex[token][docid].termfreq
                df = len(partialIndex[token])
                partialIndex[token][docid].tfidf = tf * math.log(N/df)
            
        with open(indexPath + label + ".p", "wb+") as file:
            partialIndex = pickle.dump(partialIndex, file, pickle.HIGHEST_PROTOCOL)
    
			
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
    log.warning("Building file list took %s seconds", (time.time() - start_time))
    
    #executor is gives jobs to threads
    with multithreader(max_workers = numThreads) as executor:   
        #split fileList into chunks
        for chunkNum, chunk in enumerate(chunks(fileList)):
            #give each thread a section of chunk (even distr. of big, small files)
            start_time = time.time()
            for i in range(numThreads):
                if goFast:
                    jobList.append(executor.submit(index, chunk[i::numThreads], i))
                else:
                    index(chunk[i::numThreads], i)

            futures.wait(jobList) #wait for all threads to finish
            log.warning("Processing chunk %s took %s seconds", chunkNum, (time.time() - start_time))
            dump(executor)
            
        futures.wait(jobList)
    
        #calculate tfidf
        start_time = time.time()
        N = len(fileList)
        for label in labelChunks():
            if goFast:
                jobList.append(executor.submit(calculateTFIDF, label, N))
            else:
                calculateTFIDF(label, N)
                
        futures.wait(jobList)
        log.warning("Calculating TF-IDF took %s seconds", (time.time() - start_time))


if __name__ == "__main__":
    print("LETS GOOOOOOOO")
    start = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start))
    print("PRESS ANY KEY TO QUIT")
    input()
