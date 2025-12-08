pipeline {
    agent any
    
    environment {
        SPARK_SUBMIT = '/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/bin/spark-submit'
    }
    
    stages {
        stage('1 – Balance Sheet Producer') {
            steps {
                echo "========================================"
                echo "STAGE 1: Starting Producer"
                echo "========================================"
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
                      balance-sheet/producer-balance-sheet-statement.py
                '''
                echo "✓ Producer completed - Data sent to Kafka"
            }
        }
        
        stage('2 – Balance Sheet Consumer') {
            steps {
                echo "========================================"
                echo "STAGE 2: Starting Consumer (300 rows expected)"
                echo "========================================"
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
                echo "✓ Consumer completed - Data written to Hive"
            }
        }
        
        stage('3 – Verify Results') {
            steps {
                echo "========================================"
                echo "STAGE 3: Verification"
                echo "========================================"
                sh '''
                    echo "=== PIPELINE COMPLETE ==="
                    echo ""
                    echo "Data Location: alandb.balance_sheet (Hive table)"
                    echo ""
                    echo "Verify in Hive/Hue with:"
                    echo "  SELECT COUNT(*) FROM alandb.balance_sheet;"
                    echo "  SELECT * FROM alandb.balance_sheet LIMIT 10;"
                    echo ""
                    echo "Expected: ~300 rows"
                '''
            }
        }
    }
    
    post {
        success { 
            echo "========================================"
            echo "✓✓✓ PIPELINE SUCCESS ✓✓✓"
            echo "========================================"
            echo "Producer: Data sent to Kafka"
            echo "Consumer: Data saved to alandb.balance_sheet"
        }
        failure { 
            echo "========================================"
            echo "✗✗✗ PIPELINE FAILED ✗✗✗"
            echo "========================================"
            echo "Check YARN logs for details"
        }
    }
}
