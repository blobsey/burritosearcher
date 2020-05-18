import os, time, re, gc, glob, orjson, lxml, math, binascii
try:
    import cPickle as pickle
except:
    import pickle
import logging
from collections import defaultdict 
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup
from concurrent import futures
from concurrent.futures import ProcessPoolExecutor as multithreader
from posting import Posting
from fetch import fetch

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
chunkSize = 25000

#folder containing corpus
corpusFolder = "corpus"

#folder to use as index
indexPath = "./index/" 

#folder to use to store tempIndexes
#(they will be merged into partial indexes later)
tempIndexPath = "./temp/"

# def labelChunks():
#     alphabet = "abcdefghijklmnopqrstuvwxyz"
#     for i in range(numThreads):
#         yield alphabet[i::numThreads]

def index(fileList, numThreads):
    tagList ={"h1":100, "h2":50, "h3":10, "strong":5, "b":5} #important tags
    results = list() #will hold list of partial indexes, one for each thread to process later
    for i in range(numThreads): #populate with empty indexes (dicts)
        results.append(list())
    fingerprints = list()
        
    #dont want same token in 2 diff partial indexes, so split up by first letter
    indexChooser = dict()
    for i, label in enumerate("abcdefghijklmnopqrstuvwxyz"):
        indexChooser[label] = i%numThreads
    
    #look through all files in fileList, build partialIndex
    for entry in fileList:
        filename = entry[1]
        docid = entry[2]
        with open(filename, "rb") as file:
            temp = defaultdict(int)
            site = orjson.loads(file.read()) 
            soup = BeautifulSoup(site["content"], "lxml")
            termFrequencies = defaultdict(int)
            for text in soup.find_all(text=True):
                for word in re.findall(reg, text.lower()):
                    termFrequencies[word] += 1 #record term frequency
            for importantText in soup.find_all(tagList): #weight important terms higher
                for word in re.findall(reg, str(importantText.string).lower()):
                    termFrequencies[word] += tagList[importantText.name]      
            #record all the (token, docid, termfrequency) tuples, split up so each thread can work on one
            for word in termFrequencies: 
                token = ps.stem(word) #merge words with the same stem
                temp[token] += termFrequencies[word]
            fingerprint = [4294967312] * 20
            for token, termfreq in temp.items(): 
                results[indexChooser[token[0]]].append((token, docid, termfreq)) #add document to posting list for each token 
                for i in range(0, 20):
                    x = binascii.crc32((token + str(termfreq)).encode("ascii", errors="ignore")) #hash each token+termfreq
                    a = i + 1
                    b = i + 2
                    hashed = (a*x + b) % 4294967311
                    if hashed < fingerprint[i]:
                        fingerprint[i] = hashed       
            fingerprints.append((fingerprint, docid))

    return results, fingerprints
 
#takes a partial list of (token, docid, wordfreq) tuples, dumps to a .tmp file           
def dump(partialList, threadNum):
    with open(tempIndexPath + str(threadNum) + ".tmp", "rb") as f:
        temp = pickle.load(f)
        #tokentuple is in format (token, docid, wordfreq)
        for tokentuple in partialList:
            temp[tokentuple[0]].append( (tokentuple[1], tokentuple[2]) )
    
    with open(tempIndexPath + str(threadNum) + ".tmp", "wb+") as f:
        pickle.dump(temp, f, pickle.HIGHEST_PROTOCOL)
        
    del temp
    gc.collect()

#compares each fingerprint in chunk with the whole list of fingerprints
#fingerprints are tuples of format(fingerprint, docid)
def detectDuplicates(chunk, fingerprints):
    duplicates = set() #will contain (section of) docids that are duplicates
    originals = set() #will contain ORIGINAL documents (want to keep at least one)
    for currentFingerprint in chunk:
        for otherFingerprint in fingerprints: #check each fingerprint with every other fingerprint
            if currentFingerprint[1] == otherFingerprint[1]: #dont compare a doc with itself
                break
            elif currentFingerprint[1] in originals: #dont delete the original document
                break
            else: #compare signatures
                isDuplicate = True #consider them duplicates initially
                limit = 1 #limit is how many non matching hashes to allow
                nonMatching = 0 #counts how many non matching hashes 
                for a, b in zip(currentFingerprint[0], otherFingerprint[0]):
                    if a != b:
                        nonMatching += 1
                    if nonMatching > limit: #too many non-matching hashes, they aren't duplicates
                        isDuplicate = False
                        break
                if isDuplicate:
                    duplicates.add(currentFingerprint[1])
                    originals.add(otherFingerprint[1])
                    break
    return duplicates
        
def finalizeIndex(threadNum, N, duplicates):
    #load a partialIndex, it contains a dict in format {token:list of tuples}
    #tuple is in the format (docid, wordfreq)
    #ex. to access the word freq do partialIndex[token][1]
    with open(tempIndexPath + str(threadNum) + ".tmp", "rb") as file:
        with open(indexPath + str(threadNum) + ".p", "wb") as dump:
            partialIndex = pickle.load(file)
            postings = defaultdict(list)
            positions = dict() #contains seek positions for each token in index
            #construct posting list for each token in partialIndex
            for token in partialIndex:
                positions[token] = dump.tell() #record the seek position for ez access later
                #calculate tfidf, construct posting list with docid, termfreq, tfidf
                for tokentuple in partialIndex[token]:
                    if tokentuple[0] in duplicates:
                        continue
                    tf = math.log10(tokentuple[1]) + 1
                    df = len(partialIndex[token])
                    posting = Posting(tokentuple[0], tokentuple[1], tf * math.log10(N/df))
                    postings[token].append(posting)
                pickle.dump(postings[token], dump, pickle.HIGHEST_PROTOCOL)
    
    del partialIndex
    gc.collect()
    
    #dump the positions dict
    with open(indexPath + str(threadNum) + ".positions", "wb") as dump:
        pickle.dump(positions, dump)

            
#helper function
#breaks fileList into chunks of chunkSize
#files in chunks should be evenly distributed across the fileList
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
    for i in range(numThreads):
        f = open(indexPath + str(i) + ".p", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    #make a bunch of empty position files
    for i in range(numThreads):
        f = open(indexPath + str(i) + ".positions", "wb+")
        empty = dict()
        pickle.dump(empty, f, pickle.HIGHEST_PROTOCOL)
        f.close()
        
    #make a bunch of empty temp indexes
    #each thread gets one to do its own indexing
    for i in range(numThreads):
        f = open(tempIndexPath + str(i) + ".tmp", "wb+")
        empty = defaultdict(list)
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
    
    fingerprints = list()
    duplicates = set()
    #executor gives jobs to threads
    with multithreader(max_workers = numThreads) as executor:   
        #split fileList into chunks
        for chunkNum, chunk in enumerate(chunks(fileList)):
            partialIndexes = list()
            mergedLists = list()
            for i in range(numThreads):
                mergedLists.append(list())
            start_time = time.time()
            jobList.clear()
            #give each thread a section of chunk (even distr. of big, small files)
            for i in range(numThreads):
                if goFast:
                    jobList.append(executor.submit(index, chunk[i::numThreads], numThreads))
                else:
                    result = index(chunk[i::numThreads], numThreads)
                    partialIndexes.append(result[0])
                    fingerprints += result[1]
            #each element in partialIndexes holds list of tasks for each thread
            futures.wait(jobList)
            for job in jobList:
                partialIndexes.append(job.result()[0])
                fingerprints += job.result()[1]
            log.warning("Indexing chunk %s took %s seconds", chunkNum, (time.time() - start_time))
            
            #each partialIndex has a list of small jobs pre-chosen for each thread
            #need to merge all the separate small jobs so each thread gets a full job
            for partialIndex in partialIndexes:
                for i, partialList in enumerate(partialIndex):
                        mergedLists[i] += partialList
                        
            partialIndex.clear()
            gc.collect()
            # log.warning("merging lists took %s seconds", (time.time() - start_time))
                
            start_time = time.time()
            #dump first half of partial indexes
            for i in range(int(numThreads/2)):
                if goFast:
                    job = executor.submit(dump, mergedLists[i], i)
                    jobList.append(job)
                else:
                    dump(mergedLists[i], i)
            futures.wait(jobList)
            for i in range(int(numThreads/2)):
                mergedLists[i].clear()
            gc.collect()
            #dump second half of partial indexes
            for i in range(int(numThreads/2), numThreads):
                if goFast:
                    job = executor.submit(dump, mergedLists[i], i)
                    jobList.append(job)
                else:
                    dump(mergedLists[i], i)
            futures.wait(jobList)
            for i in range(int(numThreads/2), numThreads):
                mergedLists[i].clear()
            jobList.clear()
            gc.collect()
            log.warning("dumping took %s seconds", (time.time() - start_time))

        #detect duplicates
        start_time = time.time()
        for fingerprintChunk in chunks(fingerprints):
            for i in range(numThreads):
                if goFast:
                    jobList.append(executor.submit(detectDuplicates, fingerprintChunk[i::numThreads], fingerprints))
                else:
                    duplicates = duplicates.union(detectDuplicates(fingerprintChunk[i::numThreads], fingerprints))
        futures.wait(jobList)
        for job in jobList:
            duplicates = duplicates.union(job.result())
        del fingerprints
        gc.collect()
        log.warning("duplicate detection took %s seconds", (time.time() - start_time))
                    
    start_time = time.time()
    with multithreader(max_workers = int(numThreads / 2)) as executor:
        #finalize indexes
        N = len(fileList)
        for i in range(numThreads):
            if goFast:
                job = executor.submit(finalizeIndex, i, N, duplicates)
                jobList.append(job)
            else:
                finalizeIndex(i, N, duplicates)
              
        futures.wait(jobList)
        log.warning("Finalizing indexes took %s seconds", (time.time() - start_time))
   
if __name__ == "__main__":
    yes = input("ARE YOU READY TO GO FAST (y/n)")
    if yes == "n":
        input(":(\nPRESS ANY KEY TO QUIT")
    elif yes == "y":
        print("LETS GOOOOOOOO")
        start = time.time()
        main()
        print("--- %s seconds ---" % (time.time() - start))
        input("PRESS ANY KEY TO QUIT")          
