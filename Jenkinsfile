pipeline {
    agent any

    environment {
        // Use the existing Spark 2.x from CDH 7.1.7 cluster
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('1 - Balance Sheet Producer') {
            steps {
                echo "Starting Producer with Spark"
                sh '''
                ${SPARK_SUBMIT} \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.yarn.maxAppAttempts=1 \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
                  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3 \
                  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/producer-balance-sheet-statement.py
                '''
            }
        }

        stage('2 - Balance Sheet Consumer') {
            steps {
                echo "Starting Consumer with Spark"
                sh '''
                ${SPARK_SUBMIT} \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 1g \
                  --driver-memory 1g \
                  --conf spark.yarn.maxAppAttempts=1 \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
                  --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3 \
                  --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
                  balance-sheet/consumer-balance-sheet-statement.py
                '''
            }
        }
    }

    post {
        success {
            echo "Both stages succeeded – Spark pipeline completed successfully."
        }
        failure {
            echo "Pipeline failed – please check YARN logs and Spark configuration."
        }
    }
}

