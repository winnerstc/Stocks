pipeline {
    agent any
    
    environment {
        PYSPARK_PYTHON = 'python3'
    }
    
    stages {
        stage('Debug – Balance Sheet Only') {
            steps {
                echo "=== DEBUG MODE: Balance Sheet Producer ==="
                sh '''
                spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 \
                  --executor-cores 1 \
                  --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.shuffle.service.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/producer-balance-sheet-statement.py
                '''
                
                echo "=== DEBUG MODE: Balance Sheet Consumer ==="
                sh '''
                spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 \
                  --executor-cores 1 \
                  --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.shuffle.service.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/consumer-balance-sheet-statement.py
                '''
            }
        }
    }
    
    post {
        always {
            echo "Debug run finished – check Kafka topic and HDFS/Delta for data"
        }
    }
}
