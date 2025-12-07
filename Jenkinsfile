pipeline {
    agent any
    triggers { cron('55 23 * * *') }

    stages {
        stage('=== PRODUCERS STARTING ===') {
            steps { echo "\033[1;34m\n================================================\n    PRODUCERS PHASE – STARTING NOW\n================================================\033[0m" }
        }

        stage('Producers – parallel') {
            environment { PYSPARK_PYTHON = 'python3' }
            parallel {
                stage('Balance Sheet Producer') {
                    steps {
                        echo "\033[1;32mStarting → Balance Sheet Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          balance-sheet/producer-balance-sheet-statement.py'''
                        echo "\033[1;32mFinished → Balance Sheet Producer\033[0m"
                    }
                }
                stage('Cash Flow Producer') {
                    steps {
                        echo "\033[1;32mStarting → Cash Flow Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          cash-flow/producer-cash-flow-statement.py'''
                        echo "\033[1;32mFinished → Cash Flow Producer\033[0m"
                    }
                }
                stage('Income Producer') {
                    steps {
                        echo "\033[1;32mStarting → Income Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          income/producer-income-statement.py'''
                        echo "\033[1;32mFinished → Income Producer\033[0m"
                    }
                }
            }
        }

        stage('=== ALL PRODUCERS DONE – CONSUMERS STARTING ===') {
            steps { echo "\033[1;33m\n================================================\n    ALL PRODUCERS SUCCESSFUL!\n    CONSUMERS PHASE – STARTING NOW\n================================================\033[0m" }
        }

        stage('Consumers – parallel') {
            environment { PYSPARK_PYTHON = 'python3' }
            parallel {
                stage('Balance Sheet Consumer') {
                    steps {
                        echo "\033[1;36mStarting → Balance Sheet Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          balance-sheet/consumer-balance-sheet-statement.py'''
                        echo "\033[1;36mFinished → Balance Sheet Consumer\033[0m"
                    }
                }
                stage('Cash Flow Consumer') {
                    steps {
                        echo "\033[1;36mStarting → Cash Flow Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          cash-flow/consumer-cash-flow-statement.py'''
                        echo "\033[1;36mFinished → Cash Flow Consumer\033[0m"
                    }
                }
                stage('Income Consumer') {
                    steps {
                        echo "\033[1;36mStarting → Income Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                          income/consumer-income-statement.py'''
                        echo "\033[1;36mFinished → Income Consumer\033[0m"
                    }
                }
            }
        }

        stage('=== PIPELINE COMPLETE ===') {
            steps { echo "\033[1;42m\n    FULL ETL SUCCESSFUL – ALL DATA PROCESSED!    \033[0m" }
        }
    }
}
