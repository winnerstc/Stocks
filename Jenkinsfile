pipeline {
    agent any

    environment {
        SPARK3 = '/opt/cloudera/parcels/SPARK3/bin/spark-submit'
    }

    stages {
        stage('1 – Balance Sheet Producer') {
            steps {
                echo "Starting Balance Sheet Producer (Spark 3)"
                sh '''
                ${SPARK3} \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/producer-balance-sheet-statement.py
                '''
                echo "Producer finished – data is now in Kafka"
            }
        }

        stage('2 – Balance Sheet Consumer') {
            steps {
                echo "Starting Balance Sheet Consumer (Spark 3)"
                sh '''
                ${SPARK3} \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/consumer-balance-sheet-statement.py
                '''
                echo "Consumer finished – check your target table / Delta location"
            }
        }
    }

    post {
        success { echo "Debug run completed: Producer → Consumer" }
        failure { echo "Failed – check the failing stage above" }
    }
}
