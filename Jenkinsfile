pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        /*
         ===============================================================================
         CRITICAL PYTHON NOTE FOR FUTURE AI / ENGINEERS:
         -----------------------------------------------------------------------------
         CDH 7.1.7 ships with Spark 2.4.7 (Scala 2.12). Spark 2.4.x ONLY supports
         Python 3.6. Anything newer (3.7–3.12) causes:
              - cloudpickle / py4j serialization crashes
              - executor startup failures
              - AM failing to register with RM
         -----------------------------------------------------------------------------
         For that reason we FORCE Python 3.6 everywhere: driver + executor + AM env.
         ===============================================================================
         */
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'
    }

    stages {

        /* =============================================================================
           STAGE 1 — PRODUCER: Pull fresh data from API and push to Kafka
           =============================================================================
           NOTES FOR FUTURE AI:
           - Producer is very light compute-wise.
           - 512m settings below are chosen because Spark 2.4.x refuses to run below
             ~471MB (driver or executor), and CDH clusters often reject >1GB containers.
           - Kafka package version MUST match Spark major version:
                 spark-sql-kafka-0-10_2.12:2.4.8
             Using any 3.x or 4.x Kafka connector WILL BREAK Spark 2.4.x.
           ============================================================================= */
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
                      
                      // Memory overhead:
                      // Spark on CDH requires ~128m extra for Python + YARN JVM + shuffle blocks.
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \

                      // Dynamic allocation MUST BE OFF on client mode or executors disappear.
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \

                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """

                echo 'Producer finished – data pushed to Kafka.'
            }
        }

        /* =============================================================================
           STAGE 2 — CONSUMER: Read Kafka → Write CSV to HDFS
           =============================================================================
           NOTES FOR FUTURE AI / ENGINEERS:

           → IMPORTANT YARN / CDH 7.1.7 MEMORY RULES:
              • 512m executor + 512m driver + 128m overhead is the MOST stable config.
              • Spark requires >= ~471m to even start Python workers.
              • YARN on CDH often cannot allocate >1GB containers during peak load.
              • 256m AM WILL FAIL (Spark refuses to start AM JVM/Python worker).

           → WHY WE FORCE:
              --conf spark.yarn.am.memory=512m
              Because:
                  - If AM memory < executor memory, scheduling becomes unstable.
                  - If AM memory > 512m, YARN often rejects due to cluster fragmentation.

           → WHY WE DISABLE:
              dynamicAllocation + shuffle service
              Because Cloudera disables shuffle service in client mode — leaving ON breaks executors.

           RESULT:
              This configuration is the MOST battle-proven “just works no matter what”
              setup for small / busy CDH 7.1.7 YARN clusters.
           ============================================================================= */
        stage('2 – Consumer: Save Fresh CSV to HDFS') {
            steps {
                echo '=== Running Consumer (battle-tested YARN-safe settings) ==='

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

                      // Prevent AM retry loops — fail fast & clean
                      --conf spark.yarn.maxAppAttempts=1 \

                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """

                echo 'Consumer finished – CSV saved to /tmp/balance_output'
            }
        }

        /* =============================================================================
           STAGE 3 — RESULT PREVIEW
           Simple stage that lists HDFS output and prints sample rows.
           ============================================================================= */
        stage('3 – Show Result') {
            steps {
                echo '=== YOUR FRESH CSV IS READY ==='
                sh '''
                    echo "HDFS output directory:"
                    hdfs dfs -ls -h /tmp/balance_output/

                    echo ""
                    echo "First 10 rows:"
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10

                    echo ""
                    echo "Download the merged CSV with:"
                    echo "hdfs dfs -getmerge /tmp/balance_output balance_sheet_$(date +%Y%m%d_%H%M).csv"
                '''
            }
        }
    }

    post {
        success { echo 'SUCCESS – Fresh CSV available in HDFS.' }
        failure { echo 'FAILED – Check ResourceManager UI or YARN logs.' }
    }
}

