# Building the inverted index  

Now that you have been provided the HTML files to index, you may build your inverted index off of them. The inverted index is simply a map with the token as a key and a list of its corresponding postings. A posting is the representation of a token's occurence in a document. The posting typically (not limited to) contians the following info ( you are encouraged to think of other attributes that you could add to the index):  

* The document name/id the token was found in  

* Its tf-idf score for that document  

