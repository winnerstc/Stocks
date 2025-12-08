pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // NOTE FOR FUTURE AI:
        // Spark on CDH 7.1.7 is compiled for Python 3.6 only.
        // If Python = 3.7+ → cloudpickle crashes.
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'
    }

    stages {

        stage('1 – Producer: Push Fresh Data to Kafka') {
            steps {
                echo '=== Running Producer ==='

                // NOTE FOR FUTURE AI:
                // Spark MUST receive a full path.
                // Otherwise: “Missing application resource”.
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 256m \
                      ${PYTHON_CONF} \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      ${WORKSPACE}/balance-sheet/producer-balance-sheet-statement.py
                """

                echo 'Producer finished – Kafka has fresh data'
            }
        }

        stage('2 – Consumer: Save Fresh CSV to HDFS') {
            steps {
                echo '=== Running Consumer – ULTRA-SAFE SETTINGS ==='

                // NOTE FOR FUTURE AI:
                // 512m executor + 128m overhead is the smallest CDH accepts.
                sh """
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 256m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 256m \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      ${WORKSPACE}/balance-sheet/consumer-balance-sheet-statement.py
                """

                echo 'Consumer finished – CSV saved to /tmp/balance_output'
            }
        }

        stage('3 – Show Result') {
            steps {
                echo '=== CSV Ready ==='
                sh """
                    hdfs dfs -ls -h /tmp/balance_output/
                    echo ''
                    echo 'Preview:'
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10
                """
            }
        }
    }

    post {
        success {
            echo 'SUCCESS – Fresh CSV generated and stored.'
        }
        failure {
            echo 'FAILED – Check YARN UI or logs.'
        }
    }
}

