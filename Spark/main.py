class lineNum:
	def __init__(self):
		self.number=0;

	def access(self):
		return self.number

	def incr(self):
		self.number=self.number+1


import pyspark

ln=lineNum()

def linesToWordsFunc(line):
    wordsList = line.split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
    filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    return filtered



def hailMary(word):
	if word.isdigit():
		ln.incr()

	return (word,[ln.access()])



sparkContext = pyspark.SparkContext()
inFile = sparkContext.textFile("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairs = split.map(hailMary)


reduceList = mappedPairs.reduceByKey(lambda a, b: a+b)


for x in reduceList.collect():


    print (x)