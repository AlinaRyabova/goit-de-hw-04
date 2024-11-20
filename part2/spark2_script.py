from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox") \
    .getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('./nuek-vuh3.csv')

# Перерозподіл даних
nuek_repart = nuek_df.repartition(2)

# Обробка даних
nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Проміжний action: collect
nuek_processed.collect()

# Додаткове фільтрування
nuek_processed = nuek_processed.where("count > 2")

# Знову collect після фільтрації
nuek_processed.collect()

# Остановити виконання, чекаючи натискання Enter
input("Press Enter to continue...5")

# Закриваємо сесію Spark
spark.stop()
