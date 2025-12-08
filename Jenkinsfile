pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
        PYTHON_CONF = '--conf spark.pyspark.python=/usr/bin/python3.6 ' +
                      '--conf spark.pyspark.driver.python=/usr/bin/python3.6'
        DRIVER_MEMORY = '1g'
        EXECUTOR_MEMORY = '1g'
    }

    stages {
        stage('Unit Tests') {
            steps {
                echo '=== Running Financial Pipeline Unit Tests ==='
                sh '''
                    echo "INCOME STATEMENT TESTS:"
                    python3 tests/test_producer_income_statement.py
                    
                    echo "BALANCE SHEET TESTS:"
                    python3 tests/test_producer_balance_statement.py
                    
                    echo "CASH FLOW TESTS:"
                    python3 tests/test_producer_cash_statement.py
                    
                    echo "ALL TESTS PASSED!"
                '''
            }
        }

        stage('Producer: Balance Sheet') {
            steps {
                echo '=== Running Balance Sheet Producer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/producer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Producer finished'
            }
        }

        stage('Consumer: Balance Sheet') {
            steps {
                echo '=== Running Balance Sheet Consumer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                """
                echo 'Balance Sheet Consumer finished'
            }
        }

        stage('Producer: Income Statement') {
            steps {
                echo '=== Running Income Statement Producer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/producer-income-statement.py
                """
                echo 'Income Statement Producer finished'
            }
        }

        stage('Consumer: Income Statement') {
            steps {
                echo '=== Running Income Statement Consumer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      income/consumer-income-statement.py
                """
                echo 'Income Statement Consumer finished'
            }
        }

        stage('Producer: Cash Flow') {
            steps {
                echo '=== Running Cash Flow Producer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
                      ${PYTHON_CONF} \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      cash-flow/producer-cash-flow-statement.py
                """
                echo 'Cash Flow Producer finished'
            }
        }

        stage('Consumer: Cash Flow') {
            steps {
                echo '=== Running Cash Flow Consumer (LOCAL) ==='
                sh """
                    ${SPARK_SUBMIT} \
                      --master local[*] \
                      --driver-memory ${DRIVER_MEMORY} \
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
        success { echo 'SUCCESS – All producers and consumers finished successfully (LOCAL mode)' }
        failure { echo 'FAILED – check Jenkins console output' }
    }
}

