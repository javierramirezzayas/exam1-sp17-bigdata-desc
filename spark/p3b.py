from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

# For escuelasPR.csv file
school_schema = StructType([
    StructField("school_region", StringType(), True),
    StructField("school_district", StringType(), True),
    StructField("city", StringType(), True),
    StructField("school_id", IntegerType(), True),
    StructField("school_name", StringType(), True),
    StructField("school_level", StringType(), True),
    StructField("college_board_id", IntegerType(), True)])

school_df = spark.read.csv(path="/user/jramirez/data/escuelasPR.csv",header=False,schema=school_schema)

# SQL
school_df.createOrReplaceTempView("school")

sql_query = "select school_region, school_district, city, count(*) as num_schools " \
            "from school " \
            "where school_region = 'Arecibo' " \
            "group by school_region, school_district, city " \
            "order by school_district"

spark.sql(sql_query).show()

