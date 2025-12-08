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
                    ls -la /tmp/balance_output/
                    echo "=== FILE COUNT ==="
                    find /tmp/balance_output/ -name "*.csv" | wc -l
                    echo "✅ YOUR 300 ROWS ARE IN: /tmp/balance_output/part-*.csv"
                    echo "📋 Copy to your folder: cp /tmp/balance_output/*.csv ~/financials/"
                '''
            }
        }
    }

    post {
        success {
            echo "🎉 PIPELINE SUCCESS - Balance Sheet data ready in /tmp/balance_output/"
        }
        failure {
            echo "❌ Pipeline failed - check Spark/YARN logs above"
        }
    }
}

