# Лабораторная работа №16
## Задание
1. Реализуйте парсер с использованием Selenium для сбора данных с веб-страницы. Это может быть:
онлайн-каталог
интернет-магазин
энциклопедия и т.д.
Основное требование: чтобы ресурсы не повторялись внутри группы, т.е. все должны парсить разные сайты.
2. Создайте таблицы БД и заполните их данными, полученными с помощью парсера. У вас должно быть минимум 2 таблицы. При заполнении в запросах используйте именованные плейсхолдеры драйвера вашей СУБД.
3. Напишите запросы для выборки данных из БД с использованием PyPika Query Builder. У вас должно быть:
2 запроса с JOIN
3 запроса с расчётом статистики/группировкой/агрегирующими функциями
4. Оформите отчёт в README.md. Отчёт должен содержать:
- Условия задач
- Описание проделанной работы
- Скриншоты результатов
- Ссылки на используемые материалы
### Задание 2
парсила данные с этого сайта:https://en.wikipedia.org/wiki/List_of_programming_languages
Заполненная база данных
<image src = 2.1.png alt="результат программы">
<image src = 2.2.png alt="результат программы">
<image src = 2.3.png alt="результат программы">

### Задание 3
2 запроса с JOIN:
- Языки с их категориями (первые 5)
- Количество языков в каждой категории (первые 5 категорий)
<image src = 3.1.png alt="результат программы">

3 запроса с расчётом статистики/группировкой/агрегирующими функциями:
- Общее количество языков
- Категория с наибольшим количеством языков
- Общее количество категорий
<image src = 3.2.png alt="результат программы">
### Ссылки на используемые материалы
- https://pypika.readthedocs.io/
- https://www.selenium.dev/documentation/
- https://selenium-python.readthedocs.io/