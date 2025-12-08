stage('Balance Sheet Consumer - Full Pipeline') {
    steps {
        echo "Starting Consumer with Spark - Local → HDFS Pipeline"
        sh '''
            # Clean up previous output
            rm -rf /tmp/balance-output
            
            # Run Spark consumer (saves to local /tmp)
            ${SPARK_SUBMIT} \
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
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.8 \
              --conf spark.sql.adaptive.enabled=false \
              balance-sheet/consumer-balance-sheet-statement.py
            
            # Verify local output
            echo "=== Local CSV files created ==="
            ls -la /home/Consultants/DE011025/stocks/balance-sheet/balance_output/
            
            # Auto-move to HDFS (Jenkins user can write to /tmp)
            HDFS_TARGET="hdfs://ip-172-31-8-235.eu-west-2.compute.internal:9000/tmp/DE011025/stocks-data/stocks-balance-sheet-data"
            echo "=== Copying to HDFS: $HDFS_TARGET ==="
            
            # Create HDFS directory if needed
            hdfs dfs -mkdir -p "$HDFS_TARGET"
            
            # Copy all CSV files from local to HDFS
            hdfs dfs -put -f /home/Consultants/DE011025/stocks/balance-sheet/balance_output/* "$HDFS_TARGET/"
            
            # Verify HDFS copy
            echo "=== HDFS contents ==="
            hdfs dfs -ls "$HDFS_TARGET"
            
            echo "✅ Pipeline complete: Local → HDFS automated!"
        '''
    }
}

