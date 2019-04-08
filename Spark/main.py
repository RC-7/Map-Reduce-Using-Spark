class lineNum:
	def __init__(self):
		self.number=0;

	def access(self):
		return self.number

	def incr(self):
		self.number=self.number+1


import pyspark

from nltk.corpus import stopwords
stopWords = set(stopwords.words('english'))

ln=lineNum()

def linesToWordsFunc(line):
    wordsList = line.split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
    filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    return filtered

def hailMary(word):
	if word.isdigit():
		ln.incr()
		return("",0)

	return (word,[ln.access()])


# def removeStopwordAddFormatting():
# 	fin = open("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
# 	fout = open("formattedNoStop.txt","w")
# 	currentLine=0
# 	for line in fin:
# 		for word in line.split():
# 			if word.lower() in stopWords:
# 				pass
# 			else:
# 				fout.write(" "+word.lower())
# 		fout.write(str(currentLine)+ "\n")
# 		currentLine=currentLine+1
# 	fin.close()
# 	fout.close()


def removeStopwordAddFormatting():
	fin = open("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
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
	fin = open("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
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
				
# ----------------- Inverted Index --------------------
removeStopwordAddFormatting()

sparkContext = pyspark.SparkContext()
inFile = sparkContext.textFile("formattedNoStop.txt")
lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairs = split.map(hailMary)


reduceList = mappedPairs.reduceByKey(lambda a, b: a+b)


for x in reduceList.collect():

    print (x)
# ------------------------------------------------------

removeStopword()
inFile = sparkContext.textFile("noStop.txt")
lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairsCount = split.map(lambda word: (word, 1))

reduceListCount = mappedPairsCount.reduceByKey(lambda a, b: a+b)

pls=reduceListCount.takeOrdered(20, key = lambda x: -x[1])
print (pls)

# for x in pls.collect():

#     print (x)



