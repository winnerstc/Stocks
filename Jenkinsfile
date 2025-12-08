pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
    }

    stages {
        stage('Balance Sheet Consumer - Complete Pipeline') {
            steps {
                echo "🚀 Starting Balance Sheet Consumer (300 rows expected)"
                sh '''
                    ${SPARK_SUBMIT} \
                      --master yarn --deploy-mode client \
                      --num-executors 1 \
                      --executor-cores 1 \
                      --executor-memory 512m \
                      --driver-memory 512m \
                      --conf spark.executor.memoryOverhead=128m \
                      --conf spark.driver.memoryOverhead=128m \
                      --conf spark.yarn.maxAppAttempts=1 \
                      --conf spark.dynamicAllocation.enabled=false \
                      --conf spark.sql.adaptive.enabled=false \
                      --conf spark.pyspark.python=python3 \
                      --conf spark.pyspark.driver.python=python3 \
                      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 \
                      --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=python3 \
                      --conf spark.executorEnv.PYSPARK_PYTHON=python3 \
                      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
                      balance-sheet/consumer-balance-sheet-statement.py
                '''

                echo "✅ SPARK JOB COMPLETE!"
                sh '''
                    echo "=== FILES LOCATION ==="
                    ls -la /tmp/balance_output/ 2>/dev/null || echo "No files - check Spark logs"
                    echo "=== CSV FILES ==="
                    find /tmp -path "*/balance_output/*.csv" -type f 2>/dev/null | head -5 || echo "No CSV files"
                    echo ""
                    echo "✅ SUCCESS: Balance sheet data ready!"
                    echo "📁 Location: /tmp/balance_output/part-*.csv"
                    echo "📋 Copy: cp /tmp/balance_output/*.csv ~/financials/"
                '''
            }
        }
    }

    post {
        success {
            echo "🎉 PIPELINE SUCCESS - Check /tmp/balance_output/ for your CSV files"
        }
        failure {
            echo "❌ Pipeline failed - review Spark logs above"
        }
    }
}

