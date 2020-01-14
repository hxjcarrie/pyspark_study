#!coding=utf8
'''
author: huangxiaojuan
'''
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from time import *
import numpy
import os

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf, col

def getFeatureName():
    featureLst = ['feature1', 'feature2', 'feature3', 'feature4', 'feature5', 'feature6', 'feature7', 'feature8', 'feature9']
    colLst = ['uid', 'label'] + featureLst
    return featureLst, colLst

def parseFloat(x):
    try:
        rx = float(x)
    except:
        rx = 0.0
    return rx


def getDict(dictDataLst, colLst):
    dictData = {}
    for i in range(len(colLst)):
        dictData[colLst[i]] = parseFloat(dictDataLst[i])
    return dictData

def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    # Important: asNondeterministic requires Spark 2.3 or later
    # It can be safely removed i.e.
    # return udf(to_array_, ArrayType(DoubleType()))(col)
    # but at the cost of decreased performance
    return udf(to_array_, ArrayType(DoubleType())).asNondeterministic()(col)


def main():
    #spark = SparkSession.builder.master("yarn").appName("spark_demo").getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    print "Session created!"
    sc = spark.sparkContext
    print "The url to track the job: http://namenode-01:8088/proxy/" + sc.applicationId

    sampleHDFS_train = sys.argv[1]
    sampleHDFS_test = sys.argv[2]
    outputHDFS = sys.argv[3]

    featureLst, colLst = getFeatureName()

    #读取hdfs上数据，将RDD转为DataFrame
    #训练数据
    rdd_train = sc.textFile(sampleHDFS_train)
    rowRDD_train = rdd_train.map(lambda x: getDict(x.split('\t'), colLst))
    trainDF = spark.createDataFrame(rowRDD_train)
    #测试数据
    rdd_test = sc.textFile(sampleHDFS_test)
    rowRDD_test = rdd_test.map(lambda x: getDict(x.split('\t'), colLst))
    testDF = spark.createDataFrame(rowRDD_test)

    #用于训练的特征featureLst
    vectorAssembler = VectorAssembler().setInputCols(featureLst).setOutputCol("features")

    #### 训练 ####
    print "step 1"
    lr = LogisticRegression(regParam=0.01, maxIter=100)  # regParam 正则项参数

    pipeline = Pipeline(stages=[vectorAssembler, lr])
    model = pipeline.fit(trainDF)
    #打印参数
    print "\n-------------------------------------------------------------------------"
    print "LogisticRegression parameters:\n" + lr.explainParams() + "\n"
    print "-------------------------------------------------------------------------\n"

    #### 预测, 保存结果 ####
    print "step 2"
    labelsAndPreds = model.transform(testDF).withColumn("probability_xj", to_array(col("probability"))[1])\
                                            .select("uid", "label", "prediction", "probability_xj")
    labelsAndPreds.show()
    labelsAndPreds.write.mode("overwrite").options(header="true").csv(outputHDFS + "/target/output")


    #### 评估不同阈值下的准确率、召回率
    print "step 3"
    labelsAndPreds_label_1 = labelsAndPreds.where(labelsAndPreds.label == 1)
    labelsAndPreds_label_0 = labelsAndPreds.where(labelsAndPreds.label == 0)
    labelsAndPreds_label_1.show(3)
    labelsAndPreds_label_0.show(3)
    t_cnt = labelsAndPreds_label_1.count()
    f_cnt = labelsAndPreds_label_0.count()
    print "thre\ttp\ttn\tfp\tfn\taccuracy\trecall"
    for thre in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        tp = labelsAndPreds_label_1.where(labelsAndPreds_label_1.probability_xj > thre).count()
        tn = t_cnt - tp
        fp = labelsAndPreds_label_0.where(labelsAndPreds_label_0.probability_xj > thre).count()
        fn = f_cnt - fp
        print("%.1f\t%d\t%d\t%d\t%d\t%.4f\t%.4f"%(thre, tp, tn, fp, fn, float(tp)/(tp+fp), float(tp)/(t_cnt)))

    # 保存模型
    model.write().overwrite().save(outputHDFS + "/target/model/lrModel")
    #加载模型
    #model.load(outputHDFS + "/target/model/lrModel")

    print "output:", outputHDFS


if __name__ == '__main__':
    main()

