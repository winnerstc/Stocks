pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        /*
        ==========================================================================
        PYTHON VERSION NOTE (FOR FUTURE AI/HUMANS)
        --------------------------------------------------------------------------
        Spark 2.4 (included with CDH 7.1.7) is NOT compatible with Python 3.9+.
        Cloudpickle will crash unless Python 3.6 is forced.
        ==========================================================================
        */
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'
    }

    stages {

        // =====================================================================
        // 1. PRODUCER — PUSH FRESH DATA TO KAFKA
        // =====================================================================
        stage('1 – Producer: Push Fresh Data to Kafka') {
            steps {
                echo '=== Running Producer ==='

                /*
                =====================================================================
                MEMORY NOTES FOR FUTURE AI/HUMANS
                ---------------------------------------------------------------------
                Spark 2.4 requires at least 471 MB system memory BEFORE startup.
                That is why driver-memory must be >= 512m.
                768m is chosen because it always works in real CDH clusters.
                =====================================================================
                */
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 768m \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Producer finished – fresh data is now in Kafka.'
            }
        }

        // =====================================================================
        // 2. CONSUMER — SAVE FRESH CSV TO HDFS
        // =====================================================================
        stage('2 – Consumer: Save Fresh CSV to HDFS') {
            steps {
                echo '=== Running Consumer – MINIMAL SAFE MEMORY SETTINGS ==='

                /*
                =====================================================================
                MEMORY NOTES FOR FUTURE AI/HUMANS
                ---------------------------------------------------------------------
                Spark 2.4 + CDH YARN requires:
                  • driver-memory >= 512m (471m minimum + overhead)
                  • executor-memory 256m is acceptable for lightweight jobs
                  • overhead 128m avoids container-launch failures
                  • dynamicAllocation MUST be disabled for Kafka stability
                =====================================================================
                */
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 768m \
                      --conf spark.yarn.am.memory=768m \
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

        // =====================================================================
        // 3. SHOW RESULTS
        // =====================================================================
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
        success { echo 'SUCCESS – Output ready at /tmp/balance_output' }
        failure { echo 'FAILED – Check YARN logs (ApplicationMaster likely memory issue)' }
    }
}

