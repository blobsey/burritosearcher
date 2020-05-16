class Posting:
    
    docid = 0
    termfreq = 0
    tfidf = 0.0
        
    def __init__(self, docid, termfreq, tfidf):
        self.docid = docid
        self.termfreq = termfreq
        self.tfidf = tfidf