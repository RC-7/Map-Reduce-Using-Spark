import pyspark

def linesToWordsFunc(line):
    wordsList = line.split()
    wordsList = [re.sub(r'\W+', '', word) for word in wordsList]
    filtered = filter(lambda word: re.match(r'\w+', word), wordsList)
    return filtered


sparkContext = pyspark.SparkContext()
inFile = sparkContext.textFile("/Users/TheBatComputer/Documents/DICS/ELEN4020A_Group6_Lab3/data/short.txt")
lowerCase = inFile.map(lambda x: x.lower())

split=lowerCase.flatMap(lambda line: line.split(" "))

mappedPairs = split.map(lambda word: (word, 1))
reduceList = mappedPairs.reduceByKey(lambda a, b: a + b)


for x in reduceList.collect():
    print (x)