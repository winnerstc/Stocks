pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // Force Python 3.6 to avoid cloudpickle / PySpark issues
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'

        // Spark memory settings
        DRIVER_MEMORY = '768m'
        EXECUTOR_MEMORY = '768m'
        MEMORY_OVERHEAD = '256m'
        EXECUTOR_CORES = '1'
        NUM_EXECUTORS = '1'

        // Spark Kafka package
        SPARK_KAFKA_PACKAGE = 'org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8'
    }

    stages {
        // ===========================
        // === PRODUCERS ===
        // ===========================
        stage('1 – Producer: Balance Sheet') {
            steps {
                echo '=== Running Balance Sheet Producer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Producer finished – data pushed to Kafka'
            }
        }

        stage('2 – Producer: Cash Flow') {
            steps {
                echo '=== Running Cash Flow Producer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        cash-flow/producer-cash-flow-statement.py
                """
                echo 'Cash Flow Producer finished – data pushed to Kafka'
            }
        }

        stage('3 – Producer: Income Statement') {
            steps {
                echo '=== Running Income Statement Producer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        income/producer-income-statement.py
                """
                echo 'Income Statement Producer finished – data pushed to Kafka'
            }
        }

        // ===========================
        // === CONSUMERS ===
        // ===========================
        stage('4 – Consumer: Balance Sheet') {
            steps {
                echo '=== Running Balance Sheet Consumer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Consumer finished – CSV saved to HDFS (/tmp/balance_output)'
            }
        }

        stage('5 – Consumer: Cash Flow') {
            steps {
                echo '=== Running Cash Flow Consumer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        cash-flow/consumer-cash-flow-statement.py
                """
                echo 'Cash Flow Consumer finished – CSV saved to HDFS (/tmp/cash_flow_output)'
            }
        }

        stage('6 – Consumer: Income Statement') {
            steps {
                echo '=== Running Income Statement Consumer ==='
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
                        --packages ${SPARK_KAFKA_PACKAGE} \
                        income/consumer-income-statement.py
                """
                echo 'Income Statement Consumer finished – CSV saved to HDFS (/tmp/income_statement_output)'
            }
        }

        stage('7 – Show Results') {
            steps {
                echo '=== Showing Fresh CSVs ==='
                sh '''
                    echo "Balance Sheet:"
                    hdfs dfs -ls -h /tmp/balance_output/
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10
                    echo ""

                    echo "Cash Flow:"
                    hdfs dfs -ls -h /tmp/cash_flow_output/
                    hdfs dfs -cat /tmp/cash_flow_output/part-*.csv | head -10
                    echo ""

                    echo "Income Statement:"
                    hdfs dfs -ls -h /tmp/income_statement_output/
                    hdfs dfs -cat /tmp/income_statement_output/part-*.csv | head -10
                '''
            }
        }
    }

    post {
        success { echo 'SUCCESS – Fresh CSVs for all statements are ready in /tmp/' }
        failure { echo 'FAILED – check YARN logs or ResourceManager UI (likely memory / executor configuration issue)' }
    }
}

