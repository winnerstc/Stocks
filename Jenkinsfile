pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
        
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.sql.adaptive.enabled=false ' +
                      '--conf spark.sql.adaptive.coalescePartitions.enabled=false'

        DRIVER_MEMORY = '512m'
        EXECUTOR_MEMORY = '384m'    // Further reduced
        MEMORY_OVERHEAD = '192m'    // Further reduced  
        EXECUTOR_CORES = '1'
        NUM_EXECUTORS = '1'
    }

    stages {

        stage('Producer: Balance Sheet') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Producer finished'
            }
        }

        stage('Consumer: Balance Sheet') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Consumer finished'
            }
        }

        stage('Producer: Income Statement') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/producer-income-statement.py
                """
                echo 'Income Statement Producer finished'
            }
        }

        stage('Consumer: Income Statement') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/consumer-income-statement.py
                """
                echo 'Income Statement Consumer finished'
            }
        }

        stage('Producer: Cash Flow') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      cash-flow/producer-cash-flow-statement.py
                """
                echo 'Cash Flow Producer finished'
            }
        }

        stage('Consumer: Cash Flow') {
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
                      --conf spark.default.parallelism=2 \
                      --conf spark.sql.shuffle.partitions=2 \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      cash-flow/consumer-cash-flow-statement.py
                """
                echo 'Cash Flow Consumer finished'
            }
        }

        stage('Show Balance Sheet CSV') {
            steps {
                echo '=== Showing Balance Sheet CSV ==='
                sh '''
                    hdfs dfs -ls -h /tmp/balance_output/
                    echo ""
                    echo "First 10 rows:"
                    hdfs dfs -cat /tmp/balance_output/part-*.csv | head -10
                '''
            }
        }
    }

    post {
        success { echo 'SUCCESS – All producers and consumers finished successfully' }
        failure { echo 'FAILED – check YARN logs (http://ip-172-31-3-80:8088) or kill stuck jobs: yarn application -list' }
    }
}

