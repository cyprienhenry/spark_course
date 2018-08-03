from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('FriendsByAge')
sc = SparkContext(conf = conf)

def parseLine(line):
    '''Transform input dataset line by line to extract age and number of friends
    '''

    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("file:///Users/cyprienhenry/Documents/SparkCourse/data/fakefriends.csv")
rdd = lines.map(parseLine)

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

results = averageByAge.collect()

for res in results:
    print(res)
