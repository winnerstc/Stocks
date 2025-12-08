pipeline {
    agent any

    parameters {
        booleanParam(name: 'RUN_PRODUCER', defaultValue: false, description: 'Run Stage 1 – Producer')
    }

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        PYTHON_CONF = '''
            --conf spark.pyspark.pyspark.python=/usr/bin/python3.6 \
            --conf spark.pyspark.driver.python=/usr/bin/python3.6 \
            --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 \
            --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6 \
        '''
    }

    stages {
        stage('1 – Balance Sheet Producer') {
            when {
                expression { params.RUN_PRODUCER == true }
            }
            steps {
                echo "========================================"
                echo "STAGE 1: Starting Producer"
                echo "========================================"
                sh '''
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
                      --conf spark.shuffle.service.enabled=false \
                      --conf spark.yarn.maxAppAttempts=1 \
                      --conf spark.yarn.am.waitTime=300s \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                '''
                echo "Producer completed - Data sent to Kafka"
            }
        }

        stage('2 – Balance Sheet Consumer → CSV') {
            steps {
                echo "========================================"
                echo "STAGE 2: Starting Consumer → saving CSV to /tmp/balance_output"
                echo "========================================"
                sh '''
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 1g \
                      --executor-cores 2 \
                      --num-executors 2 \
                      --driver-memory 1g \
                      ${PYTHON_CONF} \
                      --conf spark.executor.memoryOverhead=256m \
                      --conf spark.driver.memoryOverhead=256m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \
                      --conf spark.yarn.maxAppAttempts=1 \
                      --conf spark.yarn.am.waitTime=300s \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                '''
                echo "Consumer completed — CSV saved to /tmp/balance_output"
            }
        }

        stage('3 – Verify Output') {
            steps {
                echo "========================================"
                echo "STAGE 3: Verification – Your CSV is ready"
                echo "========================================"
                sh '''
                    echo "=== HDFS LISTING ==="
                    hdfs dfs -ls /tmp/balance_output/
                    echo ""
                    echo "=== FIRST 10 LINES OF CSV ==="
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10
                    echo ""
                    echo "=== DOWNLOAD COMMAND ==="
                    echo "hdfs dfs -getmerge /tmp/balance_output balance_sheet_full.csv"
                '''
            }
        }
    }

    post {
        success {
            echo "PIPELINE SUCCESS – CSV ready at /tmp/balance_output"
        }
        failure {
            echo "PIPELINE FAILED – check YARN logs"
        }
    }
}
