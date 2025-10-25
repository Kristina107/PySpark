# # Аналитика аудиокниг: ETL-пайплайн на Airflow
# 
# - Автор: Романовская Кристина
# - Дата: 22.10.2025
#
# ## Цель проекта
#
# Создание автоматизированного ETL-пайплайна для обработки данных о пользовательской активности в сервисе Яндекс Книги, 
# обеспечивающего регулярное обновление аналитических витрин для бизнес-отчётности. 
# Данная автоматизированная система, ежедневно предоставляющая актуальные данные о пользовательской активности для аналитиков сервиса, 
# ускорит подготовку отчётов и принятие продуктовых решений.
#
# ## Задачи проекта
#
# ### Архитектура и разработка:
# - Построение отказоустойчивого DAG в Apache Airflow для оркестрации процессов обработки данных
# - Настройка сенсора для мониторинга появления новых данных в S3-хранилище
# - Интеграция PySpark-приложения для выполнения распределённых вычислений
# - Организация записи результатов агрегации в базу данных ClickHouse
# 
# ### Функциональность пайплайна:
# - Ежедневный автоматический запуск обработки данных
# - Контроль качества данных на этапе поступления
# - Агрегация пользовательской активности (количество прослушиваний, среднее время)
# - Формирование витрины данных `bookmate_user_aggregate` для аналитиков
# 
# ### Техническая реализация:
# - Настройка взаимодействия между компонентами Airflow → S3 → Dataproc → ClickHouse
# - Обеспечение обработки данных за конкретные даты через параметризацию
# - Реализация мониторинга выполнения задач и обработки ошибок
# 
 
# ## 1. Написание Spark-кода
 
# filename=my_spark_job.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

# Создаём Spark-сессию и при необходимости добавляем конфигурации
spark = SparkSession.builder.appName("myAggregateTest").config("fs.s3a.endpoint", "storage.yandexcloud.net").getOrCreate()

# Указываем порт и параметры кластера ClickHouse
jdbcPort = 8443
jdbcHostname = "rc1a-3jouval14nne7aun.mdb.yandexcloud.net"
username = "da_20250921_60b107ce05"
jdbcDatabase = "playground_da_20250921_60b107ce05"
jdbcUrl = f"jdbc:clickhouse://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?ssl=true"

# Получаем аргумент из Airflow
my_date = sys.argv[1].replace('-', '_')

# Считываем исходные данные за нужную дату
df = spark.read.csv(f"s3a://da-plus-dags/script_bookmate/data_{my_date}/audition_content.csv", inferSchema=True, header=True)

# Строим агрегат по пользователям
result_df = df.groupBy("puid").agg(
    F.countDistinct("audition_id").alias("audition_count"),
    F.avg("hours").alias("avg_hours")
)

result_df.write.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("user", username) \
    .option("password", "ce1149fd61b44a47b69749c1560ee9ee") \
    .option("dbtable", "bookmate_user_aggregate") \
    .mode('append') \
    .save()


# ## 2. Создание DAG
# Spark-код готов, создаём DAG, который будет его запускать.

# filename=bookmate_dag.py

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.yandex.operators.dataproc import DataprocCreatePysparkJobOperator

class PysparkJobOperator(DataprocCreatePysparkJobOperator):
    template_fields = ("cluster_id", "args",)

DAG_ID = "audition_content_analysis"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["bookmate", "audition_analysis"]
) as dag:
    # 1) Ждём появления входного файла в S3
    wait_for_input = S3KeySensor(
        task_id="wait_for_input_file",
        bucket_name="da-plus-dags",
        bucket_key="script_bookmate/data_{{ ds.replace('-', '_') }}/audition_content.csv",
        aws_conn_id="s3",
        poke_interval=300,
        timeout=3600,
        mode="poke",
        wildcard_match=False
    )

    # 2) Запускаем PySpark-задание на кластере Dataproc (оператор Яндекс Облака)
    run_pyspark = PysparkJobOperator(
        task_id="run_pyspark_job",
        main_python_file_uri="s3a://da-plus-dags/my_spark_job.py",
        connection_id="yc_default",
        cluster_id="c9q4134h5vi546h1e148",
        args=["{{ ds }}"],
        dag=dag
    )

    # 3) Зависимости
    wait_for_input >> run_pyspark
