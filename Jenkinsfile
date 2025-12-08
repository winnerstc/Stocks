pipeline {
    agent any
    triggers { cron('55 23 * * *') }   // Daily at 23:55 + manual anytime

    stages {
        stage('=== PRODUCERS PHASE STARTING ===') {
            steps { echo "\033[1;34m\n╔══════════════════════════════════════════════════╗\n║           STARTING ALL PRODUCERS                 ║\n╚══════════════════════════════════════════════════╝\033[0m" }
        }

        stage('Producers – Running in Parallel') {
            environment { PYSPARK_PYTHON = 'python3' }
            parallel {
                stage('Balance Sheet Producer') {
                    steps {
                        echo "\033[1;32m▶ Starting → Balance Sheet Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          balance-sheet/producer-balance-sheet-statement.py'''
                        echo "\033[1;32m✔ Finished → Balance Sheet Producer\033[0m"
                    }
                }
                stage('Cash Flow Producer') {
                    steps {
                        echo "\033[1;32m▶ Starting → Cash Flow Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          cash-flow/producer-cash-flow-statement.py'''
                        echo "\033[1;32m✔ Finished → Cash Flow Producer\033[0m"
                    }
                }
                stage('Income Producer') {
                    steps {
                        echo "\033[1;32m▶ Starting → Income Producer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --conf spark.yarn.maxAppAttempts=1 \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          income/producer-income-statement.py'''
                        echo "\033[1;32m✔ Finished → Income Producer\033[0m"
                    }
                }
            }
        }

        stage('=== ALL PRODUCERS DONE – STARTING CONSUMERS ===') {
            steps { echo "\033[1;33m\n╔══════════════════════════════════════════════════╗\n║       ALL PRODUCERS SUCCESSFUL!                  ║\n║          STARTING CONSUMERS NOW                  ║\n╚══════════════════════════════════════════════════╝\033[0m" }
        }

        stage('Consumers – Running in Parallel') {
            environment { PYSPARK_PYTHON = 'python3' }
            parallel {
                stage('Balance Sheet Consumer') {
                    steps {
                        echo "\033[1;36m▶ Starting → Balance Sheet Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          balance-sheet/consumer-balance-sheet-statement.py'''
                        echo "\033[1;36m✔ Finished → Balance Sheet Consumer\033[0m"
                    }
                }
                stage('Cash Flow Consumer') {
                    steps {
                        echo "\033[1;36m▶ Starting → Cash Flow Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          cash-flow/consumer-cash-flow-statement.py'''
                        echo "\033[1;36m✔ Finished → Cash Flow Consumer\033[0m"
                    }
                }
                stage('Income Consumer') {
                    steps {
                        echo "\033[1;36m▶ Starting → Income Consumer\033[0m"
                        sh '''spark-submit \
                          --master yarn --deploy-mode client \
                          --num-executors 1 --executor-cores 2 --executor-memory 2g \
                          --driver-memory 1g \
                          --conf spark.dynamicAllocation.enabled=false \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                          income/consumer-income-statement.py'''
                        echo "\033[1;36m✔ Finished → Income Consumer\033[0m"
                    }
                }
            }
        }

        stage('=== FULL PIPELINE SUCCESS ===') {
            steps { echo "\033[1;42m\n   CONGRATULATIONS! FULL ETL COMPLETED SUCCESSFULLY  \033[0m" }
        }
    }
}
