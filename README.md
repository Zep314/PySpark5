# Spark Apache (Семинары)
## Урок 5. Spark on scala

### Условие: создайте csv файл с таким содержимым:

title,author,genre,sales,year

"1984", "George Orwell", "Science Fiction", 5000, 1949

"The Lord of the Rings", "J.R.R. Tolkien", "Fantasy", 3000, 1954

"To Kill a Mockingbird", "Harper Lee", "Southern Gothic", 4000, 1960

"The Catcher in the Rye", "J.D. Salinger", "Novel", 2000, 1951

"The Great Gatsby", "F. Scott Fitzgerald", "Novel", 4500, 1925

### Задание:

— Используя Spark прочитайте данные из файла csv.

— Фильтруйте данные, чтобы оставить только книги, продажи которых превышают 3000 экземпляров.

— Сгруппируйте данные по жанру и вычислите общий объем продаж для каждого жанра.

— Отсортируйте данные по общему объему продаж в порядке убывания.

— Выведите результаты на экран.


Решение предоставлено в файлах:

- *dz5.ipynb*

- *dz5.scala*

