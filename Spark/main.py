class lineNum:
	def __init__(self):
		self.number=0;

	def access(self):
		print(self.number)
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



# /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

def hailMary(word):
	if word.isdigit():
		ln.incr()

	return (word,ln.access())



sparkContext = pyspark.SparkContext()
inFile = sparkContext.textFile("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairs = split.map(hailMary)					# instead of putting a one put line


split2=mappedPairs.flatMap(lambda line: line.split(" "))
mappedPairs2 = split.map(lambda word: (word, 1))	
print(type(split))


# for x in mappedPairs.collect():
# 	# i=i+1
# 	# # p=x.split(" ")
# 	# p=x.map(lambda line: line.split(" "))
# 	# mappedPairs = p.map(lambda word: (word, i))
# 	print(x)

reduceList = mappedPairs.reduceByKey(lambda a, b: [a,b])
groupedList = mappedPairs.groupByKey()


for x in reduceList.collect():
    print (x)