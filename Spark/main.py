# Replace all "../data/File2ForLab3" with any of the additional files to use them


import pyspark
import time
from nltk.corpus import stopwords


class lineNum:
	def __init__(self):
		self.number=0;

	def access(self):
		return self.number

	def incr(self):
		self.number=self.number+1

stopWords = set(stopwords.words('english'))

ln=lineNum()

def linesToWordsFunc(line):
    wordsList = line.split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
    filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    return filtered

def getLineNumber(word):
	if word.isdigit():
		ln.incr()
		return("±",0)								

	return (word,[ln.access()])


def removeStopwordAddFormatting():
	fin = open("../data/File2ForLab3.txt")
	fout = open("formattedNoStop.txt","w")
	currentLine=0
	for line in fin:
		wordNo=0
		empty=False
		for word in line.split():
			if word.lower() in stopWords:
				pass
			elif word==" ":
				empty=False
				pass
			else:
				if wordNo==0:
					fout.write(word.lower())
					wordNo=wordNo+1
					empty=True
				else:
					fout.write(" "+word.lower())
					wordNo=wordNo+1
					empty=True

		if empty:
			fout.write(" "+str(currentLine)+ "\n")
			currentLine=currentLine+1
	fin.close()
	fout.close()

def removeStopword():
	fin = open("../data/File2ForLab3.txt")
	fout = open("noStop.txt","w")
	for line in fin:
		wordNo=0
		empty=False
		for word in line.split():
			if word.lower() in stopWords:
				pass
			elif word==" ":
				empty=False
				pass
			else:
				if wordNo==0:
					fout.write(word.lower())
					wordNo=wordNo+1
					empty=True
				else:
					fout.write(" "+word.lower())
					wordNo=wordNo+1
					empty=True
		if empty:
			fout.write( "\n")
	fin.close()
	fout.close()

def removeStopwordHis():
	fin = open("../data/File2ForLab3.txt")
	fout = open("noStop.txt","w")
	for line in fin:
		wordNo=0
		empty=False
		for word in line.split():
			if word.lower() in stopWords:
				pass
			elif word==" ":
				empty=False
				pass
			else:
				if wordNo==0:
					# fout.write(word.lower())
					wordNo=wordNo+1
					empty=True
				else:
					fout.write(" "+word.lower())
					wordNo=wordNo+1
					empty=True
		if empty:
			fout.write( "\n")
	fin.close()
	fout.close()
				
# ----------------- Inverted Index --------------------
# removeStopwordAddFormatting()        #Uncomment line if using any of the additional files

removeStopword()

sparkContext = pyspark.SparkContext()
# inFile = sparkContext.textFile("formattedNoStop.txt")  #Uncomment line if using any of the additional files
inFile = sparkContext.textFile("noStop.txt")    # comment out to use additional file

# inFile = sparkContext.textFile("../data/long.txt")



initial=time.time()

lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairs = split.map(getLineNumber)
final=time.time()

print("Time needed to map the inverted index algorithm: "+str(final-initial))
initial=time.time()
reduceList = mappedPairs.reduceByKey(lambda a, b: a+b)
final=time.time()
print("Time needed to reduce the inverted index algorithm: "+str(final-initial))



print("-------------------------------------------------")
print("Inverted index of words occuring")
print("Time needed to complete the inverted index algorithm: "+str(final-initial))

for x in reduceList.collect():

    print (x)
# ------------------------------------------------------


# --------------------- Top K -------------------------

removeStopwordHis() # Change to removeStopword() if using any of the additional files
inFile = sparkContext.textFile("noStop.txt")

initial=time.time()

lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairsCount = split.map(lambda word: (word, 1))

final=time.time()

print("Time needed to map the inverted index algorithm: "+str(final-initial))


initial=time.time()

reduceListCount = mappedPairsCount.reduceByKey(lambda a, b: a+b)

final=time.time()


final=time.time()
print("Time needed to reduce the inverted index algorithm: "+str(final-initial))
print("-------------------------------------------------")
print("Top 20 occuring words")

top20=reduceListCount.takeOrdered(20, key = lambda x: -x[1])      #20 is the K querry
print (top20)

print("-------------------------------------------------")
print("Top 10 occuring words")

top10=reduceListCount.takeOrdered(10, key = lambda x: -x[1])      #20 is the K querry
print (top10)

# ------------------------------------------------------



# ------------------- Sort All -------------------------

print("-------------------------------------------------")
print("Most occuring words in Descending order")

AllSorted = reduceListCount.sortBy(lambda a: a[1], ascending=False)

for x in AllSorted.collect():

    print (x)

# ------------------------------------------------------

