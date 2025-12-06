pipeline {
    agent any
    triggers { cron('55 23 * * *') }   // remove this line if you want manual only

    stages {
        stage('Producers – parallel') {
            parallel {
                stage('Balance Sheet Producer') { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 balance-sheet/producer-balance-sheet-statement.py' } }
                stage('Cash Flow Producer')    { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 cash-flow/producer-cash-flow-statement.py' } }
                stage('Income Producer')       { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 income/producer-income-statement.py' } }
            }
        }
        stage('Consumers – after all producers') {
            parallel {
                stage('Balance Sheet Consumer') { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 balance-sheet/consumer-balance-sheet-statement.py' } }
                stage('Cash Flow Consumer')    { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 cash-flow/consumer-cash-flow-statement.py' } }
                stage('Income Consumer')       { steps { sh 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 income/consumer-income-statement.py' } }
            }
        }
    }
}
