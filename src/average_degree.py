# example of program that calculates the average degree of hashtags
"""
This class generates a hashtag graph from the incoming stream of tweets
It outputs the rolling average degree of the incoming tweets within the 60 second window to the file
"""
from decimal import Decimal
import re
import sys
from datetime import datetime

class Parent:        # define parent class
   c={}
   graph={}
   def __init__(self):
      print("Calling parent constructor")

   def compareTimestamp(self,timestamp1,timestamp2):
		"""
		 This method calculates the difference in timestamp in seconds
		"""
		t1 = datetime.strptime(timestamp1[0:20]+timestamp1[26:], '%a %b %d %H:%M:%S %Y')
		t2 = datetime.strptime(timestamp2[0:20]+timestamp2[26:], '%a %b %d %H:%M:%S %Y')
		difference = t2 - t1
		seconds = difference.seconds
		#print(seconds)
		return seconds
	
   def updateDict(self,strSet):
	"""
	This method removes nodes that from the dictionary 
	"""
	self.createAdjList(strSet,"remove")

   def maintainWindow(self,data,ts):
	"""
	This method maintains the 60 sec window for the nodes
	"""
	for (tstamp,text) in data:
		if self.compareTimestamp(tstamp,ts) > 60:
			delTs, delTweet = data.pop(0)
			for txt in data:
				if all(x in txt[1] for x in delTweet):
					 return
			self.updateDict(text)
		else:
			break

   def preProcess(self,filename,fileoutput):
	"""
	 This method reads the input records, extracts hashtags and timestamp
	 and calculates the rolling averages
	"""	
	data=[]
	val =set()
	fo = open(fileoutput, "wb")
	with open(filename) as data_file:
        	for tags in data_file:
			if "timestamp" not in tags: 
        	        	 continue
			ts = re.search('timestamp: (.+?)\)', tags).group(1)
			val =set()
			val.update({tag for tag in tags.split() if tag.startswith("#")})
			#print val
			if len(val) >1:
				self.maintainWindow(data,ts)
				data.append((ts,val))
				self.createAdjList(val,"add")
				print("***")
			else:
				self.maintainWindow(data,ts)
				print("@@@@")
			result = self.calculateRollingAverages() 
			fo.write(result+"\n")
        fo.close()
        data_file.close()

   def createAdjList(self,val,operation):
	for i in val:
		for j in val:
			if (i!=j):
				if operation == "add":
					self.addNeighbor(i,j)
				else:
					self.removeNeighbor(i,j)

   def addNeighbor(self,node1,node2):
	if node1 not in self.graph:
		v2=set()
		v2.add(node2)
		self.graph[node1]=v2
	else:
		t=self.graph[node1]
		t.add(node2)
		self.graph[node1].update(t)
	if node2 not in self.graph:
		v1=set()
		v1.add(node1)
		self.graph[node2]=v1
	else:
		w=self.graph[node2]
		w.add(node1)
		self.graph[node2].update(w)
	#print(self.graph)
   
   def removeNeighbor(self,node1,node2):
	if node1 in self.graph:
		e=self.graph[node1]
		#print(e)
		if node2 in e:
			e.remove(node2)	
		if len(e) == 0:
			del self.graph[node1] 
	
 
   def calculateRollingAverages(self):
		sumKeys=0
		for key in self.graph:
			sumKeys+=len(self.graph[key])
		#print(len(self.graph))
		if sumKeys == 0 or len(self.graph) == 0:
			return str(0.00)	
		avg = '{0:.2f}'.format(round(Decimal(sumKeys)/Decimal(len(self.graph)),2))
		#print(self.graph)
		print(avg)
		return avg
			
def main():
	print(sys.argv[1])
	object=Parent()
	object.preProcess(sys.argv[1],sys.argv[2])
	
if __name__ == "__main__": main()
