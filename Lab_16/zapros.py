import sqlite3
from pypika import Table, Query, functions as fn
from pypika.terms import Order 

def execute_and_print_queries():
    conn = sqlite3.connect('languages.db')
    cursor = conn.cursor()
    language_table = Table('language')
    category_table = Table('category')
    #запрос 1
    print("Языки и их категории (топ 5):")
    query_join_1 = Query.from_(language_table)\
                        .join(category_table).on(language_table.category_id == category_table.id)\
                        .select(language_table.name, category_table.name)\
                        .limit(5)
    sql_join_1 = query_join_1.get_sql()
    cursor.execute(sql_join_1)
    results_join_1 = cursor.fetchall()
    for row in results_join_1:
        print(row)
    #запрос 2
    print("Количество языков по категориям (топ 5 категорий)")
    query_join_2 = Query.from_(category_table)\
                        .join(language_table).on(language_table.category_id == category_table.id)\
                        .select(category_table.name, fn.Count(language_table.id).as_('language_count'))\
                        .groupby(category_table.name)\
                        .limit(5)
    sql_join_2 = query_join_2.get_sql()
    cursor.execute(sql_join_2)
    results_join_2 = cursor.fetchall()
    for row in results_join_2:
        print(row)
    #запрос 3
    print("Общее количество языков:")
    query_stats_1 = Query.from_(language_table)\
                         .select(fn.Count('*').as_('total_languages'))
    sql_stats_1 = query_stats_1.get_sql()
    cursor.execute(sql_stats_1)
    result_stats_1 = cursor.fetchone() #одна строка
    print(result_stats_1)
    #запрос 4
    print("Категория с наибольшим количеством языков:")
    query_stats_2 = Query.from_(language_table)\
                         .join(category_table).on(language_table.category_id == category_table.id)\
                         .select(category_table.name, fn.Count(language_table.id).as_('language_count'))\
                         .groupby(category_table.name)\
                         .orderby(fn.Count(language_table.id), order=Order.desc)\
                         .limit(1)
    sql_stats_2 = query_stats_2.get_sql()
    cursor.execute(sql_stats_2)
    result_stats_2 = cursor.fetchone()
    print(result_stats_2)
    #запрос 5
    print("Общее количество категорий:")
    query_stats_3 = Query.from_(category_table)\
                         .select(fn.Count('*').as_('total_categories'))
    sql_stats_3 = query_stats_3.get_sql()
    cursor.execute(sql_stats_3)
    result_stats_3 = cursor.fetchone()
    print(result_stats_3)
    conn.close()
if __name__ == "__main__":
    execute_and_print_queries()