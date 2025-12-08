pipeline {
    agent any
    triggers { cron('55 23 * * *') }

    stages {
        stage('STARTING ETL – Tiny & Sequential') {
            steps { echo "\033[1;34m\nSTARTING FULL ETL – WILL RUN EVEN ON BUSY CLUSTER\033[0m" }
        }

        stage('Producers – One by One') {
            steps {
                echo "\033[1;32m1/3 Balance Sheet Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  balance-sheet/producer-balance-sheet-statement.py'''
                echo "\033[1;32mBalance Sheet Producer DONE\033[0m"

                echo "\033[1;32m2/3 Cash Flow Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  cash-flow/producer-cash-flow-statement.py'''
                echo "\033[1;32mCash Flow Producer DONE\033[0m"

                echo "\033[1;32m3/3 Income Producer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  income/producer-income-statement.py'''
                echo "\033[1;32mIncome Producer DONE\033[0m"
            }
        }

        stage('Consumers – One by One') {
            steps {
                echo "\033[1;36mStarting Balance Sheet Consumer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  balance-sheet/consumer-balance-sheet-statement.py'''
                echo "\033[1;36mBalance Sheet Consumer DONE\033[0m"

                echo "\033[1;36mStarting Cash Flow Consumer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  cash-flow/consumer-cash-flow-statement.py'''
                echo "\033[1;36mCash Flow Consumer DONE\033[0m"

                echo "\033[1;36mStarting Income Consumer\033[0m"
                sh '''spark-submit \
                  --master yarn --deploy-mode client \
                  --num-executors 1 --executor-cores 1 --executor-memory 2g \
                  --driver-memory 1g \
                  --conf spark.dynamicAllocation.enabled=false \
                  --conf spark.pyspark.python=python3 \
                  --conf spark.pyspark.driver.python=python3 \
                  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8 \
                  income/consumer-income-statement.py'''
                echo "\033[1;36mIncome Consumer DONE\033[0m"
            }
        }

        stage('SUCCESS') {
            steps { echo "\033[1;42m\nFULL ETL PIPELINE COMPLETED SUCCESSFULLY – PYTHON 3 – TINY RESOURCES!\033[0m" }
        }
    }
}
