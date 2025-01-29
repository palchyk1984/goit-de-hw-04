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

# Розподіляємо датасет на 2 частини
nuek_repart = nuek_df.repartition(2)

# Обробляємо дані
nuek_processed = nuek_repart \
    .where("final_priority < 3") \
    .select("unit_id", "final_priority") \
    .groupBy("unit_id") \
    .count()

# Фільтруємо за умовою count > 2
nuek_processed = nuek_processed.where("count > 2")

# Виводимо оброблені дані
print(nuek_processed.collect())

# Затримка перед завершенням
input("Press Enter to continue...")

# Закриваємо сесію Spark
spark.stop()