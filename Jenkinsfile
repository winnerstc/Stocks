pipeline {
    agent any
    triggers { cron('55 23 * * *') }  // 11:55 PM daily

    stages {
        stage('STARTING ETL') {
            steps {
                echo "🚀 STARTING FULL ETL – Sequential to avoid cluster overload"
                sh '''
                    # Clean up any stuck applications
                    yarn application -list | grep "jenkins" | awk "{print \$1}" | xargs -r yarn application -kill 2>/dev/null || true
                    sleep 10
                '''
            }
        }
        
        stage('Producers – Sequential NOT Parallel') {
            environment { 
                PYSPARK_PYTHON = 'python3'
                SPARK_OPTS = '--master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 512m --driver-memory 256m --conf spark.dynamicAllocation.enabled=false --conf spark.network.timeout=300s'
            }
            stages {
                stage('Balance Sheet Producer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            balance-sheet/producer-balance-sheet-statement.py
                        """
                    } 
                }
                stage('Cash Flow Producer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            cash-flow/producer-cash-flow-statement.py
                        """
                    } 
                }
                stage('Income Producer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            income/producer-income-statement.py
                        """
                    } 
                }
            }
        }
        
        stage('Wait for Producers to Complete') {
            steps {
                echo "⏳ Waiting 30 seconds for producers to finish writing to Kafka..."
                sleep 30
            }
        }
        
        stage('Consumers – Sequential NOT Parallel') {
            environment { 
                PYSPARK_PYTHON = 'python3'
                SPARK_OPTS = '--master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 512m --driver-memory 256m --conf spark.dynamicAllocation.enabled=false --conf spark.network.timeout=300s'
            }
            stages {
                stage('Balance Sheet Consumer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            balance-sheet/consumer-balance-sheet-statement.py
                        """
                    } 
                }
                stage('Cash Flow Consumer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            cash-flow/consumer-cash-flow-statement.py
                        """
                    } 
                }
                stage('Income Consumer') { 
                    steps { 
                        sh """
                            spark-submit \$SPARK_OPTS \
                            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 \
                            income/consumer-income-statement.py
                        """
                    } 
                }
            }
        }
        
        stage('Cleanup') {
            steps {
                echo "🧹 Cleaning up temporary files..."
                sh '''
                    # Optional: Clean up spark temp files
                    find /tmp -name "spark-*" -type d -mtime +1 -exec rm -rf {} \; 2>/dev/null || true
                '''
                echo "ETL Pipeline Completed Successfully!"
            }
        }
    }
}
