pipeline {
    agent any

    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
        TARGET_DIR = '/home/Consultants/DE011025/financials/balance_output'
    }

    stages {
        stage('Balance Sheet Consumer - Optimized') {
            steps {
                echo "Starting Consumer with Spark - Low Resources"
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
                
                echo "=== Verifying output in workspace ==="
                sh 'ls -la ./balance_output/ || echo "No output dir"'
                
                echo "=== Auto-copying to your directory ==="
                sh '''
                    # Create target directory if needed
                    mkdir -p ${TARGET_DIR}
                    
                    # Copy files (overwrite if exists)
                    cp -r ./balance_output/* ${TARGET_DIR}/ || echo "Copy failed - check permissions"
                    
                    # Verify final location
                    echo "=== Files in YOUR directory ==="
                    ls -la ${TARGET_DIR}/
                '''
                sh 'echo "✅ COMPLETE: Files in /home/Consultants/DE011025/financials/balance_output/"'
            }
        }
    }
}

