from pyspark import SparkConf, SparkContext
if __name__=='__main__':
    print("")
    conf = SparkConf().setMaster("local").setAppName("wordCount")
    sc = SparkContext(conf=conf)
    input = sc.textFile("input/text.txt")
    words = input.flatMap(lambda line: line.split(" "))
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x+y)
    counts = counts.sortBy(lambda x: x[1], ascending=False)
    counts.saveAsTextFile("output/wordcount")
    # output = counts.sortBy(lambda x: x[1], ascending=False).collect()
    # for word, count in output:
    #     print(word,count)