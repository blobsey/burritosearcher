# Building the inverted index  

Now that you have been provided the HTML files to index, you may build your inverted index off of them. The inverted index is simply a map with the token as a key and a list of its corresponding postings. A posting is the representation of a token's occurence in a document. The posting typically (not limited to) contians the following info ( you are encouraged to think of other attributes that you could add to the index):  

* The document name/id the token was found in  

* Its tf-idf score for that document  

Some tips:  

* When designing your inverted index, you will think about the structure of your posting first  

* You would normally begin by implementing the code to calculate/fetch the elements which will constitute your posting.  

* Modularize. Use scripts / classes that will perform a fucntion or a set of closely related functions. This helps in keeping track of your progress, debugging, and also dividing work amongst teammates if you're in a group.  

# Restrictions  

* You can use code that you or any classmate wrote for the previous projects. You cannot use code written for this project by non-group-member classmates. You are allowed to use any languages and libraries you want for text processing, including nltk. However, you are not allowed to use text indexing libraries such as Lucene, PyLucene, or ElasticSearch.

* Your index should be stored in one or more files in the file system (no databases)  

* Your indexer must load the inverted index hash map from main memory to a partial index on disk at least 3 times during index construction; those  partial indexes should be merged in the end. Optionally, after or during merging, they can also be split into separate index files with term ranges. 

# To Do  

* Make table that records the number of documents, number of unique tokens, and the size of the index on the disk.  
