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
#os.environ['PYSPARK_PYTHON'] = './python_env/py27/bin/python2' 

def parseFloat(x):
    try:
        rx = float(x)
    except:
        rx = 0.0
    return rx

def parse(line, ifUid=False):
    l = line.split('\t')
    uid = l[0]
    label = parseFloat(l[1])
    features = map(lambda x: parseFloat(x), l[2:])
    if ifUid:
        return (uid, LabeledPoint(label, features))
    else:
        return LabeledPoint(label, features)

def main():
    #spark = SparkSession.builder.master("yarn").appName("spark_demo").getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    print "Session created!"
    sc = spark.sparkContext
    print "The url to track the job: http://namenode-01:8088/proxy/" + sc.applicationId

    print sys.argv
    sampleHDFS_1 = sys.argv[1]
    sampleHDFS_2 = sys.argv[2]
    outputHDFS = sys.argv[3]

    sampleRDD = sc.textFile(sampleHDFS_1).map(parse)
    predictRDD = sc.textFile(sampleHDFS_2).map(lambda x: parse(x, True))

    # 训练
    model = LogisticRegressionWithLBFGS.train(sampleRDD)
    model.clearThreshold() #删除默认阈值（否则后面直接输出0、1）

    # 预测，保存结果
    labelsAndPreds = predictRDD.map(lambda p: (p[0], p[1].label, model.predict(p[1].features)))
    labelsAndPreds.map(lambda p: '\t'.join(map(str, p))).saveAsTextFile(outputHDFS + "/target/output")

    # 评估不同阈值下的准确率、召回率
    labelsAndPreds_label_1 = labelsAndPreds.filter(lambda lp: int(lp[1]) == 1)
    labelsAndPreds_label_0 = labelsAndPreds.filter(lambda lp: int(lp[1]) == 0)
    t_cnt = labelsAndPreds_label_1.count()
    f_cnt = labelsAndPreds_label_0.count()
    print "thre\ttp\ttn\tfp\tfn\taccuracy\trecall"
    for thre in [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        tp = labelsAndPreds_label_1.filter(lambda lp: lp[2] > thre).count()
        tn = t_cnt - tp
        fp = labelsAndPreds_label_0.filter(lambda lp: lp[2] > thre).count()
        fn = f_cnt - fp
        print("%.1f\t%d\t%d\t%d\t%d\t%.4f\t%.4f"%(thre, tp, tn, fp, fn, float(tp)/(tp+fp), float(tp)/(t_cnt)))

    # 保存模型、加载模型
    model.save(sc, outputHDFS + "/target/tmp/pythonLogisticRegressionWithLBFGSModel")
    sameModel = LogisticRegressionModel.load(sc, outputHDFS + "/target/tmp/pythonLogisticRegressionWithLBFGSModel")

    print "output:", outputHDFS


if __name__ == '__main__':
    main()

