from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .config("spark.driver.host", "127.0.0.1")\
        .getOrCreate()

# Cria um DataFrame (é tipo uma tabela virtual)
df = spark\
    .read\
    .option("multiline", "true")\
    .json("./example.json") # Abre o JSON

# Se o JSON tiver quebra de linha, precisa usar o multiline

# Mostra a estrutura do JSON
print('Estrutura do JSON')
df.printSchema()

# Mostra as 10 primeiras linhas do DataFrame
print('Prévia do conteúdo')
df.show(2)