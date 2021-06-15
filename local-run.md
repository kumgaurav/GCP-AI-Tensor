gcloud dataproc jobs submit pyspark \
"/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/my_stock_download.py" \
--cluster datapipeline-dproc-sfdc-api-log-batch-export  \
--region="us-central1" \
--project="itd-aia-dp" \
--files="/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/configs/config.yaml,/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/bq_utils.py" \
--py-files="/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/bq_utils.py"  \
--jars="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.20.0.jar" \
-- --config config.yaml


gcloud dataproc jobs submit pyspark \
"/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/symbols_by_close.py" \
--cluster datapipeline-dproc-sfdc-api-log-batch-export  \
--region="us-central1" \
--project="itd-aia-dp" \
--files="/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/configs/config.yaml,/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/bq_utils.py" \
--py-files="/Users/gkumargaur/workspace/scala/GCP-AI-Tensor/src/main/python/bq_utils.py"  \
--jars="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.20.0.jar" \
-- --config config.yaml



spark-submit \
--jars /Users/gkumargaur/workspace/scala/SparkScala31Poc/spark-bigquery-latest_2.12.jar  \
my_stock_download.py --config /Users/gkumargaur/workspace/scala/GCP-AI-Tensor/configs/config.yaml