import os, json
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from bs4 import BeautifulSoup

def parse(corpusPath):
    docID = 0
    ps = PorterStemmer()
    
    #parse all documents in corpusPath folder
    for root, directory, files in os.walk(os.path.join(os.getcwd(), corpusPath)):
        for filename in files:
            
            wordFrequencies = dict()
            
            with open(os.path.join(root, filename)) as file:
                # json files are in this format:
                #     url: url of page
                #     content: actual HTML content of webpage
                #     encoding: encoding of webpage
                document = json.loads(file.read())    
                url = document["url"]
                content = document["content"]
                encoding = document["encoding"]
                
                #use beautifulsoup to get text of webpage
                soup = BeautifulSoup(content, "html.parser")
                #get alphanumeric tokens then stem them
                tokens = [ps.stem(word) for word in word_tokenize(soup.getText()) if word.isalnum()]
                for token in tokens:
                    #build dict of words + frequencies, also save the docID for later...
                    if not token in wordFrequencies:
                        wordFrequencies[token] = [docID, 1]
                    else:
                        wordFrequencies[token][1] += 1
                
    return wordFrequencies
                    
#test code prints word frequencies
#should print: stemmed word [docID, frequency]
word_freq = parse("TEST")
for word in word_freq:
    print(word + str(word_freq[word]))
        