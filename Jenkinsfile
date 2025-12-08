pipeline {
    agent any
    triggers { cron('55 23 * * *') }

    stages {
        stage('PRODUCERS – Running ONE BY ONE (fast & reliable)') {
            steps {
                echo "\033[1;34mStarting PRODUCERS sequentially – no resource fighting\033[0m"

                echo "\033[1;32m1/3 Balance Sheet Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  balance-sheet/producer-balance-sheet-statement.py'''
                echo "Balance Sheet Producer DONE"

                echo "\033[1;32m2/3 Cash Flow Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  cash-flow/producer-cash-flow-statement.py'''
                echo "Cash Flow Producer DONE"

                echo "\033[1;32m3/3 Income Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  income/producer-income-statement.py'''
                echo "Income Producer DONE"
            }
        }

        stage('CONSUMERS – Running sequentially') {
            steps {
                echo "\033[1;33mStarting CONSUMERS\033[0m"

                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  balance-sheet/consumer-balance-sheet-statement.py'''

                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  cash-flow/consumer-cash-flow-statement.py'''

                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 2 --executor-cores 3 --executor-memory 4g \
                  --driver-memory 2g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  income/consumer-income-statement.py'''
            }
        }

        stage('SUCCESS') {
            steps { echo "\033[1;42mFULL ETL COMPLETED SUCCESSFULLY – ALL IN PYTHON 3!\033[0m" }
        }
    }
}
