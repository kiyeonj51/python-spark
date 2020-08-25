from pyspark import SparkConf, SparkContext
import wget
import os
if __name__=='__main__':
    conf = SparkConf().setMaster("local").setAppName("pageRank")
    sc = SparkContext(conf=conf)
    if not os.path.isfile('input/web.txt.gz'):
        url = 'https://snap.stanford.edu/data/web-BerkStan.txt.gz'
        wget.download(url, 'input/web.txt.gz')
    # Assume that our neighbor list was saved as a Spark objectFile
    input = sc.textFile("input/web.txt.gz").filter(lambda line: '#' not in line)
    # links = input.map(lambda line: line.split("\t")).groupByKey().mapValues(list)
    links = input.map(lambda line: line.split("\t")).groupByKey().mapValues(list).partitionBy(100).persist()
    ranks = links.mapValues(lambda x: 1.0)
    for i in range(10):
        print(f"{i}-iteration of PageRank")
        link_rank = links.join(ranks)
        contributions = link_rank.map(lambda x: (x[1][0], x[1][1] / len(x[1][0]))).flatMap(lambda row: [(link, row[1]) for link in row[0]])
        ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda v: 0.15 + 0.85 * v)
    ranks.saveAsTextFile("output/rank")