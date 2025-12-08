pipeline {
    agent any

    environment {
        PYSPARK_PYTHON = 'python2'
    }

    triggers {
        cron('55 23 * * *') // daily at 23:55
    }

    stages {
        stage('Balance Sheet Producer & Consumer') {
            steps {
                echo 'Running Balance Sheet Producer...'
                sh 'spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 2G --executor-cores 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 balance-sheet/producer-balance-sheet-statement.py'

                echo 'Running Balance Sheet Consumer...'
                sh 'spark-submit --master yarn --deploy-mode client --num-executors 2 --executor-memory 2G --executor-cores 1 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 balance-sheet/consumer-balance-sheet-statement.py'
            }
        }
    }
}
