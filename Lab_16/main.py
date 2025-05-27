import time
import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from pypika import Table, Query, functions as fn
from pypika.terms import Order

def parse_languages():
    url = "https://en.wikipedia.org/wiki/List_of_programming_languages"

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(url)
    time.sleep(2)#загрузка страницы

    language_elements = driver.find_elements(By.CSS_SELECTOR, "div.div-col li")
    languages = []
    for item in language_elements:
        name = item.text.strip()
        if name and len(name) < 100:
            languages.append(name)
    driver.quit()
    return languages

def prepare_db():
    conn = sqlite3.connect('languages.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS category (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS language (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category_id INTEGER,
        FOREIGN KEY (category_id) REFERENCES category(id)
    );
    ''')

    conn.commit()
    return conn, cursor

def fill_db(languages, cursor, conn):
    categories = sorted(set(l[0].upper() for l in languages if l[0].isalpha()))#группирую языки по первой букве
 
    for cat in categories:
        cursor.execute('INSERT OR IGNORE INTO category(name) VALUES (:name)', {'name': cat})
    conn.commit()

    cursor.execute('SELECT id, name FROM category')
    cat_map = {row[1]: row[0] for row in cursor.fetchall()}

    for lang in languages:
        first_char = lang[0].upper()
        if first_char in cat_map:
            cursor.execute(
                'INSERT INTO language(name, category_id) VALUES (:name, :category_id)', 
                {'name': lang, 'category_id': cat_map[first_char]}
            )
    conn.commit()

def pypika_queries():
    language = Table('language')
    category = Table('category')
#JOIN-запросы
    join_1 = Query.from_(language).join(category).on(language.category_id == category.id)\
    .select(language.name, category.name).limit(5)
    join_2 = Query.from_(category).join(language).on(language.category_id == category.id)\
    .select(category.name, fn.Count(language.id).as_('cnt')).groupby(category.name).limit(5)
#запросы с расчётом статистики/группировкой/агрегирующими функциями
    stats_1 = Query.from_(language).select(fn.Count('*').as_('total_languages'))
    stats_2 = Query.from_(language).join(category).on(language.category_id == category.id)\
        .select(category.name, fn.Count(language.id).as_('cnt'))\
        .groupby(category.name).orderby(fn.Count(language.id), order=Order.desc).limit(1)
    stats_3 = Query.from_(category).select(fn.Count('*').as_('total_categories'))
    print("-- JOIN-запрос 1 --\n", join_1.get_sql())
    print("-- JOIN-запрос 2 --\n", join_2.get_sql())
    print("-- Стат.запрос 1 --\n", stats_1.get_sql())
    print("-- Стат.запрос 2 --\n", stats_2.get_sql())
    print("-- Стат.запрос 3 --\n", stats_3.get_sql())

def main():
    print("Загрузка списка языков программирования с Wikipedia...")
    languages = parse_languages()
    print(f"Найдено языков: {len(languages)}")

    print("Создание базы данных и таблиц...")
    conn, cursor = prepare_db()

    print("Заполнение БД данными...")
    fill_db(languages, cursor, conn)

    print("Примеры запросов с PyPika:")
    pypika_queries()

    conn.close()
    print("Работа завершена.")

if __name__ == "__main__":
    main()