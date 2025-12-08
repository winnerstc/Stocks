pipeline {
    agent any

    environment {
        // This path exists on every CDH 7.1.7 cluster that has Spark 3 parcel activated
        SPARK3_SUBMIT = '/opt/cloudera/parcels/SPARK3_ON_YARN/bin/spark-submit'
    }

    stages {
        stage('1 – Balance Sheet Producer') {
            steps {
                echo "Starting Producer with Spark 3"
                sh '''
                ${SPARK3_SUBMIT} \
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

        stage('2 – Balance Sheet Consumer') {
            steps {
                echo "Starting Consumer with Spark 3"
                sh '''
                ${SPARK3_SUBMIT} \
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
        success { echo "Both stages succeeded – Spark 3 working perfectly" }
        failure { echo "Check the path or YARN logs" }
    }
}
