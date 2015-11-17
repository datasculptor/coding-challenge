# example of program that calculates the number of tweets cleaned
"""
This Python class processes one line from the input file and outputs the extracted
values to the output file
At the end it outputs the count of the total tweets that contained unicode
"""
import json
import sys

class Parent:
 def __init__(self):
        print("Calling Parent")

 def cleanTweets(self,filename,fileoutput):
        count=0
        fo = open(fileoutput, "wb")
        with open(filename) as data_file:                  
                for line in data_file:
                        val = json.loads(line)
                        #print(val)
                        if "created_at" not in val:
                                continue
                        createDate = val["created_at"]
                        tweet=val["text"]
                        cleanTweet=unicode(tweet).encode('ascii','ignore')
                        #print(createDate,tweet)
                        fo.write(cleanTweet+" (timestamp: "+createDate+")"+"\n")
                        #print(len(tweet),len(cleanTweet))
                        if (len(tweet) > len(cleanTweet)):
                                count+=1
                print(str(count)+" tweets contained unicode.")
                fo.write("\n"+str(count)+" tweets contained unicode.")
                fo.close()
                data_file.close()

def main():
        print(sys.argv[1])
        object=Parent()
        object.cleanTweets(sys.argv[1],sys.argv[2])

if __name__ == "__main__": main()
