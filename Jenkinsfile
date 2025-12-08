pipeline {
    agent any

    environment {
        SPARK3 = '/opt/cloudera/parcels/SPARK3/bin/spark-submit'
    }

    stages {
        stage('1 – Balance Sheet Producer') {
            steps {
                echo "Running Producer – Spark 3 on CDH"
                sh '''
                ${SPARK3} \
                  --master yarn \
                  --deploy-mode client \
                  --num-executors 1 \
                  --executor-cores 1 \
                  --executor-memory 1g \
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
                echo "Running Consumer – Spark 3 on CDH"
                sh '''
                ${SPARK3} \
                  --master yarn \
                  --deploy-mode client \
                  --num-executors 1 \
                  --executor-cores 1 \
                  --executor-memory 1g \
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
        success { echo "Done – both stages succeeded" }
        failure { echo "Failed – check YARN logs for the container that died" }
    }
}
