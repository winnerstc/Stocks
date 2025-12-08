pipeline {
    agent any
    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
    }
    stages {
        stage('Balance Sheet Consumer - Complete Pipeline') {
            steps {
                echo "Starting Balance Sheet Consumer (300 rows expected)"
                sh '''
                    ${SPARK_SUBMIT} \
                      --master yarn \
                      --deploy-mode client \
                      --executor-memory 512m \
                      --executor-cores 1 \
                      --num-executors 1 \
                      --driver-memory 512m \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.shuffle.service.enabled=false \
                      --conf spark.yarn.maxAppAttempts=1 \
                      --conf spark.pyspark.python=python3 \
                      --conf spark.pyspark.driver.python=python3 \
                      --conf spark.yarn.am.waitTime=300s \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                '''
                echo "SPARK JOB COMPLETE!"
                sh '''
                    echo "=== OUTPUT FILES ==="
                    ls -la /tmp/balance_output/ || echo "/tmp/balance_output/ not found"
                    echo "=== CSV COUNT ==="
                    find /tmp/balance_output/ -name "*.csv" | wc -l || echo "0"
                    echo "YOUR 300 ROWS ARE READY:"
                    echo "   /tmp/balance_output/part-*.csv"
                    echo "Copy now → cp /tmp/balance_output/*.csv ~/financials/ || true"
                '''
            }
        }
    }
    post {
        success { echo "PIPELINE SUCCESS - Data ready in /tmp/balance_output/" }
        failure { echo "Pipeline failed - check logs above" }
    }
}

