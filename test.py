import os, glob

allFiles = []

for name in glob.glob(".\TEST2\**\*.json", recursive=True): 
    print(name)
    