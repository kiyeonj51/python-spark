from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
import os
import csv
from io import StringIO
import json
import wget

if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("tutorial")

    print('Basic RDD transformation')
    sc = SparkContext(conf=conf)
    lines = sc.textFile('README.md')
    nums = sc.parallelize([1, 2, 3, 3])
    items = ['lines','nums','map','flatMap','filter','distinct','sample']
    map = lines.map(lambda x: (x.split(" ")[0], x))
    flatMap = nums.flatMap(lambda x: list(range(x, 4)))
    filter = lines.filter(lambda line: "Python" in line)
    distinct = nums.distinct()
    sample = nums.sample(False, 0.5)
    for item in items:
        print(f"{item}:{eval(item).take(10)}")
    sc.stop()


    print('\n\nTwo-RDD Transformation')
    sc = SparkContext(conf=conf)
    nums = sc.parallelize([1, 2, 3, 3])
    others = sc.parallelize([3, 3, 6, 7])
    items = ['nums','others','union','intersection','subtract','cartesian']
    union = nums.union(others)
    intersection = nums.intersection(others)
    subtract = nums.subtract(others)
    cartesian = nums.cartesian(others)
    for item in items:
        print(f"{item}:{eval(item).take(10)}")
    sc.stop()


    print('\n\nBasic action on an RDD')
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 3])
    items =['rdd','collect','count','countByValue','take','top','takeOrdered','takeSample',
            'reduce','fold','aggregate','foreach']
    collect = rdd.collect()
    count = rdd.count()
    countByValue = rdd.countByValue().items()
    take = rdd.take(2)
    top = rdd.top(2)
    takeOrdered = rdd.takeOrdered(2)
    takeSample = rdd.takeSample(False, 1)
    reduce = rdd.reduce(lambda x, y: x + y)
    fold = rdd.fold(0, lambda x, y: x + y)
    aggregate = rdd.aggregate((0, 0),
                              (lambda acc, value: (acc[0] + value, acc[1] + 1)),
                              (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1]))
                              )
    def square(x):
        return x * x
    foreach = rdd.foreach(lambda x: square(x))
    for item in items:
        print(f"{item}:{eval(item)}")
    sc.stop()


    print('\n\nTransformation on one pair RDD')
    sc = SparkContext(conf=conf)
    pairs = sc.parallelize([(1, 2), (3, 4), (3, 6)])
    items = ['pairs','reduceByKey', 'groupByKey', 'combineByKey',
             'mapValues', 'flatMapValues', 'keys', 'values', 'sortByKey']
    reduceByKey = pairs.reduceByKey(lambda x, y: x + y)
    groupByKey = pairs.groupByKey().map(lambda x: (x[0], list(x[1])))
    sumCount = pairs.combineByKey((lambda x: (x, 1)),
                                      (lambda x, y: (x[0] + y, x[1] + 1)),
                                      (lambda x, y: (x[0] + y[0], x[1] + y[1])))
    combineByKey = sumCount.map(lambda key_xy: (key_xy[0], key_xy[1][0]/key_xy[1][1]))
    mapValues = pairs.mapValues(lambda x: x + 1)
    flatMapValues = pairs.flatMapValues(lambda x: list(range(x, 6)))
    keys = pairs.keys()
    values = pairs.values()
    sortByKey = pairs.sortByKey()
    for item in items:
        print(f"{item}:{eval(item).collect()}")
    sc.stop()

    print('\n\nTransformations on two pair RDDs')
    sc = SparkContext(conf=conf)
    pair1 = sc.parallelize([(1,2),(3,4),(3,6)])
    pair2 = sc.parallelize([(3,9)])
    items = ['pair1', 'pair2', 'subtractByKey', 'join',
             'rightOuterJoin', 'leftOuterJoin', 'cogroup']
    subtractByKey = pair1.subtractByKey(pair2)
    join = pair1.join(pair2)
    rightOuterJoin = pair1.rightOuterJoin(pair2)
    leftOuterJoin = pair1.leftOuterJoin(pair2)
    cogroup = pair1.cogroup(pair2).map(lambda x: (x[0], (list(x[1][0]), list(x[1][1]))))
    for item in items:
        print(f"{item}:{eval(item).collect()}")
    sc.stop()

    print('\n\nActions on pair RDDs')
    sc = SparkContext(conf=conf)
    pairs = sc.parallelize([(1, 2), (3, 4), (3, 6)])
    items = ['pairs','countByKey', 'collectAsMap','lookup']
    countByKey = pairs.countByKey().items()
    collectAsMap = pairs.collectAsMap()
    lookup = pairs.lookup(3)
    for item in items:
        print(f"{item}:{eval(item)}")
    sc.stop()

    print('\n\nLoad dataset')
    sc = SparkContext(conf=conf)
    input = sc.textFile('input/pandas.txt')
    data = input.map(lambda x: json.loads(x))
    if not os.path.isdir('output/lovespandas'):
        data.filter(lambda x: x['lovesPandas']).map(lambda x: json.dumps(x)).saveAsTextFile('output/lovespandas')

    def loadRecord(line):
        input = StringIO(line)
        reader = csv.DictReader(input, fieldnames=['name','favoriteAnimal'])
        return next(reader)
    input = sc.textFile('input/csv.txt').map(loadRecord)
    print(f'csv.txt:{input.collect()}')

    def loadRecords(fileNameContents):
        input = StringIO(fileNameContents[1])
        reader = csv.DictReader(input, fieldnames=["name", "favoriteAnimal"])
        return reader
    fullFileData = sc.wholeTextFiles('input/csv.txt').flatMap(loadRecords)
    print(f'csv.txt:{fullFileData.collect()}')

    def writeRecords(records):
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=["name", "favoriteAnimal"])
        for record in records:
            writer.writerow(record)
        return [output.getvalue()]

    if not os.path.isdir('output/csv'):
        fullFileData.mapPartitions(writeRecords).saveAsTextFile('output/csv')
    sc.stop()


    print('\n\nCreate sql table with txt file')
    sc = SparkContext(conf=conf)
    url = 'https://raw.githubusercontent.com/apache/spark/master/examples/src/main/resources/kv1.txt'
    inputFile = 'input/kv1.txt'
    inputTable = 'myTable'
    if not os.path.isfile(inputFile):
        wget.download(url, inputFile)
    hiveCtx = HiveContext(sc)
    if not os.path.isdir(f'spark-warehouse/{inputTable}'):
        hiveCtx.sql(f"CREATE TABLE IF NOT EXISTS {inputTable} (key INT, value STRING)")
        hiveCtx.sql(f"LOAD DATA LOCAL INPATH '{inputFile}' INTO TABLE myTable")
    input = hiveCtx.sql(f"FROM {inputTable} SELECT key, value")
    print(f'myTable query: {input.rdd.take(10)}')
    print(f'myTable key: {input.rdd.map(lambda row: row[0]).take(10)}')
    sc.stop()

    print('\n\nRead json file into table')
    sc = SparkContext(conf=conf)
    hiveCtx = HiveContext(sc)
    tweets = hiveCtx.read.json('input/tweets.json')
    tweets.registerTempTable("tweets")
    results = hiveCtx.sql("SELECT user.name, text FROM tweets")
    resultsText = results.rdd.map(lambda row: row.text)
    print(f'tweets query: {results.collect()}')
    print(f'tweets text: {resultsText.collect()}')
    sc.stop()




