from pyspark import SparkConf, SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
if __name__=='__main__':
    conf = SparkConf().setMaster("local").setAppName("spamClassify")
    sc = SparkContext(conf=conf)
    spam = sc.textFile("input/spam.txt")
    normal = sc.textFile("input/normal.txt")

    # Create a HashingTF instance to map email text to vectors of 10,000 features.
    tf = HashingTF(numFeatures=10000)
    # Each email is split into words, and each word is mapped to one feature.
    spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
    normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))

    # Create LabeledPoint datasets for positive (spam) and negative (normal) examples.

    positiveExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
    negativeExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
    trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache()  # Cache since Logistic Regression is an iterative algorithm.
    # Run Logistic Regression using the SGD algorithm.
    model = LogisticRegressionWithSGD.train(trainingData)
    # Test on a positive example (spam) and a negative one (normal). We first apply
    # the same HashingTF feature transformation to get vectors, then apply the model.
    posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    print("Prediction for positive test example: %g" % model.predict(posTest))
    print("Prediction for negative test example: %g" % model.predict(negTest))
    sc.stop()
