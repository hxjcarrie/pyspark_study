source ./conf.sh

ModelType=df
if [ $# -gt 0 ];then
    if [ $1 == "sql" ];then
        ModelType=df_sql
    fi
fi
echo -e "\n--------------------\nModelType: ${ModelType}\n--------------------\n"

CUR_PATH=$(cd "$(dirname "$0")";pwd)
echo $CUR_PATH

sample_path=hdfs:///user/huangxiaojuan/program/dfDemo/input/sample
school_path=hdfs:///user/huangxiaojuan/program/dfDemo/input/school
job_path=hdfs:///user/huangxiaojuan/program/dfDemo/input/job
output_path=hdfs:///user/huangxiaojuan/program/dfDemo/output/${ModelType}

hadoop fs -rmr $output_path

${SPARK_PATH}/bin/spark-submit \
  --master yarn \
  --name "spark_demo_${ModelType}_${who}" \
  --queue ${YARN_QUEUE} \
  --deploy-mode ${DEPLOY_MODE} \
  --driver-memory 2g \
  --driver-cores 1 \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 5 \
  --archives ${sourceDIR}/py27.zip#python_env \
  --conf spark.default.parallelism=20 \
  --conf spark.executor.memoryOverhead=4g \
  --conf spark.driver.memoryOverhead=2g \
  --conf spark.yarn.maxAppAttempts=3 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.pyspark.driver.python=${sourceDIR}/py27/bin/python2 \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python_env/py27/bin/python2 \
  --conf spark.pyspark.python=./python_env/py27/bin/python2 \
  ./${ModelType}.py $sample_path $school_path $job_path $output_path
