

def minhash(words): #words should be dict with tokens as key and wordfreq as value
    vector = list()
            

    for i in range(0, 200):
        currentMin = 4294967312
        for word in words:
            x = binascii.crc32((word + str(words[word])).encode("ascii", errors="ignore"))
            a = i + 1
            b = i + 2
            minNum = 4294967311
            for x in hashes:
                hashed = (a*x + b) % 4294967311
                if hashed < minNum:
                    minNum = hashed
                
        vector.append(minNum)
    
    return tuple(vector)










