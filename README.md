# Tarea3
#Este código usa el archivo de datos y399-rzwf.csv (Colombianos registrados en el exterior)
#La consulta realizada es por la edad, identificando los mayores a 35 años y con la selección de columnas País y Ciudad de residencia donde se encuentran registrados.
#Y finalmente ordenar los valores de forma descendente de acuerdo a la edad del colombiano registrado en el exterior.

#Importamos librerias necesarias 
from pyspark.sql import SparkSession, functions as F 
# Inicializa la sesión de Spark 
spark = SparkSession.builder.appName('Tarea3').getOrCreate() 
# Define la ruta del archivo .csv en HDFS 
file_path = 'hdfs://localhost:9000/Tarea3/y399-rzwf.csv' 
# Lee el archivo .csv 
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path) 
#imprimimos el esquema 
df.printSchema() 
# Muestra las primeras filas del DataFrame 
df.show() 
# Estadisticas básicas 
df.summary().show() 
# Consulta: Filtrar por valor y seleccionar columnas 
print("Edad con valor mayor a 35\n") 
dias = df.filter(F.col('edad_a_os') > 35).select('edad_a_os','pas_S','ciudad_de_residencia') 
dias.show() 
# Ordenar filas por los valores en la columna "edad_a_os" en orden descendente 
print("Valores ordenados de mayor a menor\n") 
sorted_df = df.sort(F.col("edad_a_os").desc())
sorted_df.show()


