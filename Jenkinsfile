pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // ================================================================
        // PYTHON VERSION NOTE (FOR FUTURE AI/HUMANS)
        // ------------------------------------------------
        // CDH 7.1.7 ships Spark 2.4.0 which is NOT compatible with Python 3.9+
        // Must force Python 3.6 or cloudpickle will crash your job.
        // ================================================================
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
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \

                      // =====================================================
                      // MEMORY RULES FOR FUTURE AI/HUMANS:
                      // -----------------------------------------------------
                      // Spark 2.4 requires *at least 471 MB* of system memory.
                      // That means driver-memory MUST be >= 512m.
                      // If driver < 512m → Spark fails before job starts.
                      // =====================================================

                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 768m \   // SAFE VALUE – WORKS EVERYWHERE

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
                echo '=== Running Consumer – MINIMAL YET SAFE MEMORY SETTINGS ==='

                // ===============================================================
                // MEMORY NOTES FOR FUTURE AI / HUMANS:
                // ---------------------------------------------------------------
                // Spark/YARN rules:
                //   • System memory ≥ 471 MB ALWAYS REQUIRED by Spark 2.4.
                //   • 768m driver-memory is the smallest that ALWAYS works.
                //   • Executor can stay small (256m) because compute load is low.
                //   • Overhead 128m prevents container-launch failures.
                //   • dynamicAllocation MUST be disabled on CDH 7.1.7 since
                //     Kafka streaming + DA = unstable.
                // ===============================================================

                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \

                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 768m \   // REQUIRED FOR STABILITY

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
        failure { echo 'FAILED – Check YARN logs (ApplicationMaster error likely memory-related)' }
    }
}

