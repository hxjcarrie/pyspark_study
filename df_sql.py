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


def isIntByRegex(numStr):
    try:
        int(numStr)
        return True
    except:
        return False

def NowDate():
    return strftime('%Y-%m-%d %H:%M:%S', localtime())

def main():
    #spark = SparkSession.builder.master("yarn").appName("spark_demo").getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    print "Session created!"
    sc = spark.sparkContext
    print "The url to track the job: http://namenode-01:8088/proxy/" + sc.applicationId

    sampleHDFS = sys.argv[1]
    schoolHDFS = sys.argv[2]
    jobHDFS = sys.argv[3]
    outputHDFS = sys.argv[4]

    # 读取样本表, 从RDD转为dataFrame
    sampleRDD = sc.textFile(sampleHDFS).map(lambda x: x.split("\t")) \
                    .filter(lambda x: isIntByRegex(x[2]) and isIntByRegex(x[5]) and isIntByRegex(x[4])) \
                    .map(lambda x: (x[0], x[2], x[5], x[4], x[1]))
    schemaSample = StructType(map(lambda column: StructField(column, StringType(), True), "bg,UserID,s_JobId,s_SchoolId,action".split(",")))
    rowSample = sampleRDD.map(lambda line: Row(line[0], line[1], line[2], line[3], line[4]))
    sampleDataFrame = spark.createDataFrame(rowSample, schemaSample)
    print "\n" + NowDate()
    sampleDataFrame.show(5)
    print NowDate() + '\n'
    sampleDataFrame.createOrReplaceTempView("sampleTable")

    # 读取job表, 从RDD转为dataFrame
    jobRDD = sc.textFile(jobHDFS).map(lambda x: x.split("\t")).filter(lambda x: len(x) > 50).map(lambda x: (x[0], x[2]))
    schemaJob = StructType(map(lambda column: StructField(column, StringType(), True), "JobId,regionId".split(",")))
    rowJob = jobRDD.map(lambda line: Row(line[0], line[1]))
    jobDataFrame = spark.createDataFrame(rowJob, schemaJob)
    print "\n" + NowDate()
    jobDataFrame.show(5)
    print NowDate() + '\n'
    jobDataFrame.createOrReplaceTempView("jobTable")

    # 读取school表，本身就是parquet格式, 所以直接load完就是dataFrame
    schoolDataFrame = spark.read.option("header", True).load(schoolHDFS).select("id", "user_id", "city_code", "region_code")
    print "\n" + NowDate()
    schoolDataFrame.show(5)
    print NowDate() + '\n'
    schoolDataFrame.createOrReplaceTempView("schoolTable")

    #关联
    #dataFrame = sampleDataFrame\
    #     .where(sampleDataFrame.bg == 0)\
    #     .join(jobDataFrame, sampleDataFrame.s_JobId == jobDataFrame.JobId, "left")\
    #     .join(schoolDataFrame, sampleDataFrame.s_SchoolId == schoolDataFrame.id, "left")\
    #     .selectExpr("regionId", "region_code", "city_code", "bg", "UserID", "s_JobId", "s_SchoolId", "action")

    dataFrame = spark.sql("select regionId, region_code, city_code, bg, UserID, s_JobId, s_SchoolId, action \
                           from sampleTable \
                           left join jobTable on sampleTable.s_JobId == jobTable.JobId \
                           left join schoolTable on sampleTable.s_SchoolId == schoolTable.id \
                           where sampleTable.bg == 0")

    dataFrame.show()
    #dataFrame.write.mode("overwrite").options(header="true").csv(outputHDFS + "/csv")
    dataFrame.write.mode("overwrite").options(header="true").save(outputHDFS + "/parquet")


    print "output: " + outputHDFS

if __name__ == '__main__':
    main()

