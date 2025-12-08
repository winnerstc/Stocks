pipeline {
    agent any

    environment {
        // =========================
        // Spark submit path
        // =========================
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'

        // =========================
        // Enforce Python 3.6 to avoid cloudpickle / PySpark version issues
        // =========================
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3.6 ' +
                      '--conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6'

        // =========================
        // Resource settings tuned for your cluster:
        // 3 nodes, 8GB each, 4–8 vcores
        // =========================
        DRIVER_MEMORY = '512m'
        EXECUTOR_MEMORY = '512m'
        MEMORY_OVERHEAD = '256m'
        EXECUTOR_CORES = '1'
        NUM_EXECUTORS = '1'  // safe for YARN max container limits
    }

    stages {

        // =========================
        // 1 – Balance Sheet Producer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Producer finished'
            }
        }

        // =========================
        // 2 – Balance Sheet Consumer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Consumer finished'
            }
        }

        // =========================
        // 3 – Income Statement Producer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/producer-income-statement.py
                """
                echo 'Income Statement Producer finished'
            }
        }

        // =========================
        // 4 – Income Statement Consumer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/consumer-income-statement.py
                """
                echo 'Income Statement Consumer finished'
            }
        }

        // =========================
        // 5 – Cash Flow Producer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      cash-flow/producer-cash-flow-statement.py
                """
                echo 'Cash Flow Producer finished'
            }
        }

        // =========================
        // 6 – Cash Flow Consumer
        // =========================
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
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      cash-flow/consumer-cash-flow-statement.py
                """
                echo 'Cash Flow Consumer finished'
            }
        }

        // =========================
        // 7 – Show Result of Balance Sheet CSV
        // =========================
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
        failure { echo 'FAILED – check YARN logs or ResourceManager UI (likely memory / executor configuration issue)' }
    }
}

