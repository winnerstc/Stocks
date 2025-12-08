pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // CRITICAL: Forces Python 3.6 → avoids cloudpickle / PySpark version issues
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'

        // All Spark memory settings safely above minimum required by Spark 2.4
        DRIVER_MEMORY = '768m'
        EXECUTOR_MEMORY = '768m'
        MEMORY_OVERHEAD = '256m'
        EXECUTOR_CORES = '1'
        NUM_EXECUTORS = '1'
    }

    stages {
        stage('1 – Producer: Push Fresh Data to Kafka') {
            steps {
                echo '=== Running Producer ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory ${EXECUTOR_MEMORY} \
                      --executor-cores ${EXECUTOR_CORES} \
                      --num-executors ${NUM_EXECUTORS} \
                      --driver-memory ${DRIVER_MEMORY} \
                      --conf spark.executor.memoryOverhead=${MEMORY_OVERHEAD} \
                      --conf spark.driver.memoryOverhead=${MEMORY_OVERHEAD} \
                      --conf spark.dynamicAllocation.enabled=false \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Producer finished – fresh data pushed to Kafka'
            }
        }

        stage('2 – Consumer: Save Fresh CSV to HDFS') {
            steps {
                echo '=== Running Consumer ==='
                // NOTES FOR FUTURE AI / HUMANS:
                //   • Executor and driver memory = 768m → above Spark 2.4 minimum (471 MB)
                //   • Memory overhead = 256m → avoids ApplicationMaster crashes on YARN
                //   • Python 3.6 enforced → avoids PySpark serialization issues
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory ${EXECUTOR_MEMORY} \
                      --executor-cores ${EXECUTOR_CORES} \
                      --num-executors ${NUM_EXECUTORS} \
                      --driver-memory ${DRIVER_MEMORY} \
                      --conf spark.executor.memoryOverhead=${MEMORY_OVERHEAD} \
                      --conf spark.driver.memoryOverhead=${MEMORY_OVERHEAD} \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \
                      --conf spark.yarn.am.memory=${DRIVER_MEMORY} \
                      --conf spark.yarn.am.cores=${EXECUTOR_CORES} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Consumer finished – CSV saved to HDFS (/tmp/balance_output)'
            }
        }

        stage('3 – Show Result') {
            steps {
                echo '=== Showing Fresh CSV ==='
                sh '''
                    hdfs dfs -ls -h /tmp/balance_output/
                    echo ""
                    echo "First 10 rows:"
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10
                    echo ""
                    echo "Download with:"
                    echo "hdfs dfs -getmerge /tmp/balance_output balance_sheet_$(date +%Y%m%d_%H%M).csv"
                '''
            }
        }
    }

    post {
        success { echo 'SUCCESS – Fresh CSV is ready at /tmp/balance_output' }
        failure { echo 'FAILED – check YARN logs or ResourceManager UI (likely memory / executor configuration issue)' }
    }
}

