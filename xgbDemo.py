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

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from sparkxgb import XGBoostClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
#from sparkxgb import XGBoostEstimator
from sparkxgb import XGBoostRegressor

#os.environ['PYSPARK_PYTHON'] = './python_env/py27/bin/python2' 
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars xgboost4j-spark-0.72.jar,xgboost4j-0.72.jar pyspark-shell'


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

def parse(line):
    l = line.split('\t')
    label = parseFloat(l[0])
    features = map(lambda x: parseFloat(x), l[1:])
    return LabeledPoint(label, features)


def getDict(dictDataLst, colLst):
    dictData = {}
    for i in range(len(colLst)):
        dictData[colLst[i]] = parseFloat(dictDataLst[i])
    return dictData

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

    ## 训练
    print "step 1"
    xgboost = XGBoostRegressor(featuresCol="features", labelCol="label", predictionCol="prediction",
                               numRound=50, colsampleBylevel=0.7, trainTestRatio=0.9,
                               subsample=0.7, seed=123, missing = 0.0, evalMetric="rmse")

    pipeline = Pipeline(stages=[vectorAssembler, xgboost])
    model = pipeline.fit(trainDF)

    ## 预测, 保存结果
    print "step 2"
    labelsAndPreds = model.transform(testDF).select("uid", "label", "prediction")
    labelsAndPreds.write.mode("overwrite").options(header="true").csv(outputHDFS + "/target/output")

    print "step 3"
    # 评估不同阈值下的准确率、召回率
    labelsAndPreds_label_1 = labelsAndPreds.where(labelsAndPreds.label == 1)
    labelsAndPreds_label_0 = labelsAndPreds.where(labelsAndPreds.label == 0)
    labelsAndPreds_label_1.show(3)
    labelsAndPreds_label_0.show(3)
    t_cnt = labelsAndPreds_label_1.count()
    f_cnt = labelsAndPreds_label_0.count()
    print "thre\ttp\ttn\tfp\tfn\taccuracy\trecall"
    for thre in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]:
        tp = labelsAndPreds_label_1.where(labelsAndPreds_label_1.prediction > thre).count()
        tn = t_cnt - tp
        fp = labelsAndPreds_label_0.where(labelsAndPreds_label_0.prediction > thre).count()
        fn = f_cnt - fp
        print("%.1f\t%d\t%d\t%d\t%d\t%.4f\t%.4f"%(thre, tp, tn, fp, fn, float(tp)/(tp+fp), float(tp)/(t_cnt)))

    # 保存模型
    model.write().overwrite().save(outputHDFS + "/target/model/xgbModel")
    #加载模型
    #model.load(outputHDFS + "/target/model/xgbModel")

    print "output:", outputHDFS


if __name__ == '__main__':
    main()

