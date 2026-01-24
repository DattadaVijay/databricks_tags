# Databricks notebook source
#test - add
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

# COMMAND ----------
schema = StructType([
    StructField("customer_id", StringType(), True, metadata={"comment": "Unique customer identifier"}),
    StructField("first_name", StringType(), True, metadata={"comment": "Customer first name"}),
    StructField("last_name", StringType(), True, metadata={"comment": "Customer last name"}),
    StructField("email", StringType(), True, metadata={"comment": "Customer email address, PII"}),
    StructField("phone_number", StringType(), True, metadata={"comment": "Customer phone number"}),
    StructField("date_of_birth", StringType(), True, metadata={"comment": "Customer date of birth"}),
    StructField("country", StringType(), True, metadata={"comment": "Customer demographic data"})
])

data = [
    ("C001","John","Adams","john.adams@example.com","555-201-1111","1985-01-12","US"),
    ("C002","Sarah","Brown","sarah.brown@example.com","555-202-1112","1990-04-22","CA"),
    ("C003","Michael","Clark","michael.clark@example.com","555-203-1113","1978-09-10","US"),
    ("C004","Emily","Davis","emily.davis@example.com","555-204-1114","1992-07-14","UK"),
    ("C005","David","Evans","david.evans@example.com","555-205-1115","1983-03-03","US"),
    ("C006","Lisa","Foster","lisa.foster@example.com","555-206-1116","1989-12-25","IN"),
    ("C007","James","Griffin","james.griffin@example.com","555-207-1117","1995-11-28","US"),
    ("C008","Olivia","Harris","olivia.harris@example.com","555-208-1118","1987-06-19","CA"),
    ("C009","Daniel","Irwin","daniel.irwin@example.com","555-209-1119","1975-05-01","US"),
    ("C010","Sophia","Jones","sophia.jones@example.com","555-210-1120","1993-10-20","AU"),
    ("C011","Ethan","Keller","ethan.keller@example.com","555-211-1121","1988-02-11","US"),
    ("C012","Chloe","Lopez","chloe.lopez@example.com","555-212-1122","1991-09-30","CA"),
    ("C013","Ryan","Morris","ryan.morris@example.com","555-213-1123","1984-08-07","US"),
    ("C014","Ava","Nelson","ava.nelson@example.com","555-214-1124","1996-03-14","UK"),
    ("C015","Noah","Owens","noah.owens@example.com","555-215-1125","1982-01-19","US"),
    ("C016","Emma","Parker","emma.parker@example.com","555-216-1126","1989-05-23","US"),
    ("C017","Liam","Quinn","liam.quinn@example.com","555-217-1127","1994-08-29","CA"),
    ("C018","Mia","Reed","mia.reed@example.com","555-218-1128","1986-02-05","US"),
    ("C019","Mason","Stewart","mason.stewart@example.com","555-219-1129","1990-12-12","US"),
    ("C020","Isabella","Turner","isabella.turner@example.com","555-220-1130","1992-04-09","US"),
    ("C021","Jacob","Underwood","jacob.underwood@example.com","555-221-1131","1987-06-02","US"),
    ("C022","Harper","Vance","harper.vance@example.com","555-222-1132","1995-09-17","CA"),
    ("C023","Benjamin","White","benjamin.white@example.com","555-223-1133","1980-11-26","US"),
    ("C024","Ella","Xu","ella.xu@example.com","555-224-1134","1993-08-11","CN"),
    ("C025","Logan","Young","logan.young@example.com","555-225-1135","1984-07-03","US"),
    ("C026","Aria","Zimmer","aria.zimmer@example.com","555-226-1136","1991-05-15","US"),
    ("C027","Henry","Adler","henry.adler@example.com","555-227-1137","1988-03-22","UK"),
    ("C028","Grace","Benson","grace.benson@example.com","555-228-1138","1996-10-04","US"),
    ("C029","Samuel","Carter","samuel.carter@example.com","555-229-1139","1983-02-28","US"),
    ("C030","Lily","Dawson","lily.dawson@example.com","555-230-1140","1994-12-21","CA"),
    ("C031","Wyatt","Ellis","wyatt.ellis@example.com","555-231-1141","1985-09-15","US"),
    ("C032","Scarlett","Fleming","scarlett.fleming@example.com","555-232-1142","1992-11-07","US"),
    ("C033","Jack","Gibson","jack.gibson@example.com","555-233-1143","1990-06-18","US"),
    ("C034","Victoria","Hayes","victoria.hayes@example.com","555-234-1144","1987-03-27","CA"),
    ("C035","Owen","Iverson","owen.iverson@example.com","555-235-1145","1993-01-30","US"),
    ("C036","Nora","Jennings","nora.jennings@example.com","555-236-1146","1989-08-24","US"),
    ("C037","Elijah","King","elijah.king@example.com","555-237-1147","1994-10-31","US"),
    ("C038","Hannah","Lewis","hannah.lewis@example.com","555-238-1148","1985-04-16","UK"),
    ("C039","Lucas","Mitchell","lucas.mitchell@example.com","555-239-1149","1991-12-02","US"),
    ("C040","Zoey","Norton","zoey.norton@example.com","555-240-1150","1995-03-09","US")
]

df = spark.createDataFrame(data, schema)

df.write.mode("overwrite").saveAsTable("pii_demo_raw_data")
