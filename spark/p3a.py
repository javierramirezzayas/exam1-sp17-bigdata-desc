from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

# For studentPR.csv file
student_schema = StructType([
    StructField("school_region", StringType(), True),
    StructField("school_district", StringType(), True),
    StructField("school_id", IntegerType(), True),
    StructField("school_name", StringType(), True),
    StructField("school_level", StringType(), True),
    StructField("student_gender", StringType(), True),
    StructField("student_id", IntegerType(), True)])

student_df = spark.read.csv(path="/user/jramirez/data/studentsPR.csv",header=False,schema=student_schema)

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
student_df.createOrReplaceTempView("student")

school_df.createOrReplaceTempView("school")

sql_query = "select A.student_id, A.school_name, B.city " \
            "from student as A, school as B " \
            "where A.school_id=B.school_id " \
            "and (B.city = 'Ponce' or B.city = 'San Juan') " \
            "and A.student_gender='M' " \
            "and B.school_level='Superior' " \
            "order by B.city"

spark.sql(sql_query).show()

