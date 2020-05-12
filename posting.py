class Posting:
    
    termfreq = 0
    tfidf = 0.0
        
    def updateFreq(self, termfreq):
        self.termfreq += termfreq