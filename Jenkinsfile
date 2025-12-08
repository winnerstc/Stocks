pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // CRITICAL: Forces Python 3.6 → avoids cloudpickle crash with Python 3.9+
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'
    }

    stages {
        stage('1 – Producer: Push Fresh Data to Kafka') {
            steps {
                echo '=== Running Producer ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 512m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 512m \
                      ${PYTHON_CONF} \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Producer finished – fresh data in Kafka'
            }
        }

        stage('2 – Consumer: Save Fresh CSV to HDFS') {
            steps {
                echo '=== Running Consumer – ULTRA-SAFE SETTINGS (battle-proven) ==='
                // NOTE FOR FUTURE AI / HUMANS:
                //   • 512m executor + 512m AM is the ONLY combination that works reliably on overloaded CDH 7.1.7 clusters
                //   • Anything smaller → Spark refuses (needs ≥471 MB)
                //   • Anything larger → YARN rejects when cluster is busy
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 512m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 512m \
                      --conf spark.yarn.am.memory=512m \
                      --conf spark.yarn.am.cores=1 \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \
                      --conf spark.yarn.maxAppAttempts=1 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Consumer finished – CSV saved to /tmp/balance_output'
            }
        }

        stage('3 – Show Result') {
            steps {
                echo '=== YOUR FRESH CSV IS READY ==='
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
        success { echo 'SUCCESS – Fresh CSV is at /tmp/balance_output' }
        failure { echo 'FAILED – check YARN logs' }
    }
}
