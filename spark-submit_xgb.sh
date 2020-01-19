source ./conf.sh

#ModelType=lrDemo
ModelType=xgbDemo

CUR_PATH=$(cd "$(dirname "$0")";pwd)
echo $CUR_PATH

input_path_train=hdfs:///user/huangxiaojuan/program/sparkDemo/input/train
input_path_test=hdfs:///user/huangxiaojuan/program/sparkDemo/input/test
output_path=hdfs:///user/huangxiaojuan/program/sparkDemo/${ModelType}

hadoop fs -rmr $output_path


${SPARK_PATH}/bin/spark-submit \
  --master yarn \
  --name "spark_${ModelType}_${who}" \
  --queue ${YARN_QUEUE} \
  --deploy-mode ${DEPLOY_MODE} \
  --driver-memory 6g \
  --driver-cores 4 \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 5 \
  --archives ${sourceDIR}/py27.zip#python_env \
  --py-files ${sourceDIR}/pyspark-xgboost/sparkxgb.zip \
  --jars ${sourceDIR}/pyspark-xgboost/xgboost4j-spark-0.90.jar,./source/pyspark-xgboost/xgboost4j-0.90.jar \
  --conf spark.default.parallelism=150 \
  --conf spark.executor.memoryOverhead=4g \
  --conf spark.driver.memoryOverhead=2g \
  --conf spark.yarn.maxAppAttempts=3 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --conf spark.pyspark.driver.python=${sourceDIR}/py27/bin/python2 \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./python_env/py27/bin/python2 \
  --conf spark.pyspark.python=./python_env/py27/bin/python2 \
  ./${ModelType}.py $input_path_train $input_path_test $output_path

