/*
chcp 65001 && spark-shell -i \Work\dz5.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

/*
Spark Apache (Семинары)
Урок 5. Spark on scala
Условие: создайте csv файл с таким содержимым:

title,author,genre,sales,year

"1984", "George Orwell", "Science Fiction", 5000, 1949
"The Lord of the Rings", "J.R.R. Tolkien", "Fantasy", 3000, 1954
"To Kill a Mockingbird", "Harper Lee", "Southern Gothic", 4000, 1960
"The Catcher in the Rye", "J.D. Salinger", "Novel", 2000, 1951
"The Great Gatsby", "F. Scott Fitzgerald", "Novel", 4500, 1925

Задание:
— Используя Spark прочитайте данные из файла csv.
— Фильтруйте данные, чтобы оставить только книги, продажи которых превышают 3000 экземпляров.
— Сгруппируйте данные по жанру и вычислите общий объем продаж для каждого жанра.
— Отсортируйте данные по общему объему продаж в порядке убывания.
— Выведите результаты на экран.
*/

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File

// spark-shell
// :silent


val t1 = System.currentTimeMillis()

// Создаем dataframe
val df_data = sc.parallelize(List(
    ("1984", "George Orwell", "Science Fiction", 5000, 1949),
    ("The Lord of the Rings", "J.R.R. Tolkien", "Fantasy", 3000, 1954),
    ("To Kill a Mockingbird", "Harper Lee", "Southern Gothic", 4000, 1960),
    ("The Catcher in the Rye", "J.D. Salinger", "Novel", 2000, 1951),
    ("The Great Gatsby", "F. Scott Fitzgerald", "Novel", 4500, 1925)
        )).toDF("title", "author", "genre", "sales", "year")
df_data.show()

// Записываем файл в папку '~Work/data'
df_data.coalesce(1)
    .write
    .option("header", "true")
    .mode("overwrite")
    .csv("~Work/data")


// Определяем имя файла в каталоге
val directory = new File("~Work/data");
val files = directory.listFiles().filter(_.getName.endsWith(".csv"));
val f = files.head.getName;

// Читаем данные из файла
var df = spark.read.format("csv").option("header","true").load("~Work/data/" + f)

// Конвертируем данные string -> int
df = df.withColumn("sales",df("sales").cast("int")).withColumn("year",df("year").cast("int"))

println("======== Readind data :========")
df.show()

// Фильтруем данные, чтобы оставить только книги, продажи которых превышают 3000 экземпляров.
println("======== Filter data : ========")
val df_sales = df.filter(df("sales") > 3000)
df_sales.show()

// Сгруппируйте данные по жанру и вычислите общий объем продаж для каждого жанра.
println("======== Group data : =========")
val df_group = df.groupBy("genre").agg(sum("sales").alias("total_sum"))
df_group.show()


// Отсортируйте данные по общему объему продаж в порядке убывания.
println("======== Sort data : ==========")
val df_sort = df.sort(desc("sales"))
df_sort.show()


val s0 = (System.currentTimeMillis() - t1)/1000;
val s = s0 % 60;
val m = (s0/60) % 60;
val h = (s0/60/60) % 24;

// Время выполнения
println("Spent time: %02d:%02d:%02d".format(h, m, s));

System.exit(0)

/*
======================== Результат выполнения ============================

user@spark:~$ /opt/spark/bin/spark-shell
24/04/27 16:59:43 WARN Utils: Your hostname, spark resolves to a loopback address: 127.0.1.1; using 192.168.10.32 instead (on interface ens18)
24/04/27 16:59:43 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.4.1
      /_/

Using Scala version 2.13.8 (OpenJDK 64-Bit Server VM, Java 17.0.8)
Type in expressions to have them evaluated.
Type :help for more information.
24/04/27 16:59:49 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.10.32:4040
Spark context available as 'sc' (master = local[*], app id = local-1714226390422).
Spark session available as 'spark'.

scala> :silent

scala> :load /home/user/Work/dz5.scala
Loading /home/user/Work/dz5.scala...
+--------------------+-------------------+---------------+-----+----+
|               title|             author|          genre|sales|year|
+--------------------+-------------------+---------------+-----+----+
|                1984|      George Orwell|Science Fiction| 5000|1949|
|The Lord of the R...|     J.R.R. Tolkien|        Fantasy| 3000|1954|
|To Kill a Mocking...|         Harper Lee|Southern Gothic| 4000|1960|
|The Catcher in th...|      J.D. Salinger|          Novel| 2000|1951|
|    The Great Gatsby|F. Scott Fitzgerald|          Novel| 4500|1925|
+--------------------+-------------------+---------------+-----+----+

======== Readind data :========
+--------------------+-------------------+---------------+-----+----+
|               title|             author|          genre|sales|year|
+--------------------+-------------------+---------------+-----+----+
|                1984|      George Orwell|Science Fiction| 5000|1949|
|The Lord of the R...|     J.R.R. Tolkien|        Fantasy| 3000|1954|
|To Kill a Mocking...|         Harper Lee|Southern Gothic| 4000|1960|
|The Catcher in th...|      J.D. Salinger|          Novel| 2000|1951|
|    The Great Gatsby|F. Scott Fitzgerald|          Novel| 4500|1925|
+--------------------+-------------------+---------------+-----+----+

======== Filter data : ========
+--------------------+-------------------+---------------+-----+----+
|               title|             author|          genre|sales|year|
+--------------------+-------------------+---------------+-----+----+
|                1984|      George Orwell|Science Fiction| 5000|1949|
|To Kill a Mocking...|         Harper Lee|Southern Gothic| 4000|1960|
|    The Great Gatsby|F. Scott Fitzgerald|          Novel| 4500|1925|
+--------------------+-------------------+---------------+-----+----+

======== Group data : =========
+---------------+---------+
|          genre|total_sum|
+---------------+---------+
|Southern Gothic|     4000|
|          Novel|     6500|
|        Fantasy|     3000|
|Science Fiction|     5000|
+---------------+---------+

======== Sort data : ==========
+--------------------+-------------------+---------------+-----+----+
|               title|             author|          genre|sales|year|
+--------------------+-------------------+---------------+-----+----+
|                1984|      George Orwell|Science Fiction| 5000|1949|
|    The Great Gatsby|F. Scott Fitzgerald|          Novel| 4500|1925|
|To Kill a Mocking...|         Harper Lee|Southern Gothic| 4000|1960|
|The Lord of the R...|     J.R.R. Tolkien|        Fantasy| 3000|1954|
|The Catcher in th...|      J.D. Salinger|          Novel| 2000|1951|
+--------------------+-------------------+---------------+-----+----+

Spent time: 00:00:11
user@spark:~$

*/