import psycopg2
from faker import Faker
import random
from typing import List, Tuple, Optional

# ====================================================================
# КОНФІГУРАЦІЯ БАЗИ ДАНИХ (ОБОВ'ЯЗКОВО ЗАМІНИТИ!)
# ====================================================================
DB_CONFIG = {
    "dbname": "oid_lab0",
    "user": "postgres",  # <-- ЗАМІНІТЬ!
    "password": "AB87povb_09s",  # <-- ЗАМІНІТЬ!
    "host": "localhost",
    "port": "5432"
}
NUM_RECORDS_TO_INSERT = 10
TABLE_NAME = "users"


# ====================================================================
# ДОПОМІЖНІ ФУНКЦІЇ ДЛЯ ПІДКЛЮЧЕННЯ ТА ВИВОДУ
# ====================================================================

def get_connection():
    """Створює та повертає об'єкт підключення до БД."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"ПОМИЛКА: Помилка підключення до бази даних: {e}")
        return None


def print_table(conn: psycopg2.connect, limit: int, title: str):
    """Виводить зразок таблиці (останні 'limit' записів або перші 3) у консоль."""
    if limit > 0:
        # Вивід останніх N записів
        sql = f"SELECT id, username, email, age FROM {TABLE_NAME} ORDER BY id DESC LIMIT {limit};"
    else:
        # Вивід перших 3 записів (частинка таблиці)
        sql = f"SELECT id, username, email, age FROM {TABLE_NAME} ORDER BY id ASC LIMIT 3;"

    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            results = cur.fetchall()

            print("\n" + "=" * 80)
            print(f"### {title} (Виведено {len(results)} записів)")
            print(f"{'ID':<5} {'USERNAME':<25} {'EMAIL':<40} {'AGE':<5}")
            print("-" * 80)
            for user in results:
                print(f"{user[0]:<5} {user[1]:<25} {user[2]:<40} {user[3]:<5}")
            print("=" * 80 + "\n")
    except psycopg2.Error as e:
        print(f"ПОМИЛКА: Помилка вибірки для виводу таблиці: {e}")


# ====================================================================
# ГЕНЕРАЦІЯ ТА ВСТАВКА ДАНИХ (10 ЗАПИСІВ АНГЛІЙСЬКОЮ)
# ====================================================================

def generate_fake_data(num_records: int) -> List[Tuple]:
    """Генерує список кортежів із фейковими даними на EN, де username відповідає email."""
    fake = Faker('en_US')
    fake_users = []
    used_usernames = set()

    while len(fake_users) < num_records:
        name_part = fake.first_name().lower()
        random_suffix = random.randint(10, 999)
        base_username = f"{name_part}{random_suffix}"

        if base_username in used_usernames:
            continue

        used_usernames.add(base_username)

        # Створення email на основі username
        email = f"{base_username}@testcompany.org"
        age = random.randint(18, 65)

        fake_users.append((base_username, email, age))

    return fake_users


def insert_batch_data(data: List[Tuple]):
    """Вставляє дані пакетно (для первинного наповнення)."""
    conn = get_connection()
    if conn is None: return

    insert_query = f"""
        INSERT INTO {TABLE_NAME} (username, email, age) 
        VALUES (%s, %s, %s)
        ON CONFLICT (username) DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            cur.executemany(insert_query, data)
            conn.commit()
            print(f"УСПІХ: [INIT] Успішно додано {cur.rowcount} нових записів.")
    except psycopg2.Error as e:
        print(f"ПОМИЛКА: Помилка пакетної вставки даних: {e}")
        conn.rollback()
    finally:
        if conn: conn.close()


# ====================================================================
# CRUD ОПЕРАЦІЇ
# ====================================================================

def execute_crud_operation(sql: str, params: tuple = None, operation_name: str = "Query") -> Optional[int]:
    """Виконує CRUD операції (окрім SELECT) та повертає ID (для INSERT)."""
    conn = get_connection()
    if conn is None: return None

    returning_id = None
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if operation_name == "INSERT":
                returning_id = cur.fetchone()[0]
            conn.commit()
            print(f"УСПІХ: [{operation_name}] Операція успішна.")
    except psycopg2.Error as e:
        print(f"ПОМИЛКА: [{operation_name}] Помилка виконання: {e}")
        conn.rollback()
    finally:
        if conn: conn.close()
    return returning_id


# ====================================================================
# ОСНОВНА ДЕМОНСТРАЦІЯ
# ====================================================================

if __name__ == '__main__':

    # 0. Початкове наповнення: Створюємо 10 записів
    print("\n" + "=" * 80)
    print(f"--- 0. ГЕНЕРАЦІЯ: Вставляємо {NUM_RECORDS_TO_INSERT} записів (username/email збігаються) ---")
    insert_batch_data(generate_fake_data(NUM_RECORDS_TO_INSERT))

    # 1. ПЕРШИЙ SELECT: Виведення всієї таблиці (останні 10)
    conn_select = get_connection()
    if conn_select:
        print_table(conn_select, limit=10, title="1. SELECT: Стан таблиці після початкового наповнення")
        conn_select.close()

    # 1.1. ОКРЕМИЙ SELECT: Виведення частинки таблиці (перших 3 записи)
    conn_select_part = get_connection()
    if conn_select_part:
        print_table(conn_select_part, limit=0, title="1.1. SELECT: Вивід частини таблиці (перші 3 записи)")
        conn_select_part.close()

    # 2. INSERT: Додаємо новий тестовий запис
    print("\n" + "=" * 80)
    print("--- 2. INSERT: Додавання одного тестового запису ---")
    TEST_USERNAME = "FINAL_TESTER_100"
    sql_insert = f"INSERT INTO {TABLE_NAME} (username, email, age) VALUES (%s, %s, %s) RETURNING id;"
    new_user_id = execute_crud_operation(
        sql=sql_insert,
        params=(TEST_USERNAME, f"{TEST_USERNAME}@testcompany.org", 100),
        operation_name="INSERT"
    )

    # 2.1. SELECT: Вивід таблиці після INSERT
    if new_user_id:
        test_id = new_user_id
        conn_post_insert = get_connection()
        if conn_post_insert:
            print_table(conn_post_insert, limit=11,
                        title=f"2.1. SELECT: Стан таблиці після INSERT (додано ID: {test_id})")
            conn_post_insert.close()

        # 3. UPDATE: Зміна email
        print("\n" + "=" * 80)
        print(f"--- 3. UPDATE: Зміна email для ID {test_id} ---")
        new_email_value = f"UPDATED_FINAL_{test_id}@newdomain.com"
        sql_update = f"UPDATE {TABLE_NAME} SET email = %s WHERE id = %s;"
        execute_crud_operation(
            sql=sql_update,
            params=(new_email_value, test_id),
            operation_name="UPDATE"
        )

        # 3.1. SELECT: Вивід таблиці після UPDATE
        conn_post_update = get_connection()
        if conn_post_update:
            print_table(conn_post_update, limit=11,
                        title=f"3.1. SELECT: Стан таблиці після UPDATE (змінено ID: {test_id})")
            conn_post_update.close()

        # 4. DELETE: Видалення тестового запису
        print("\n" + "=" * 80)
        print(f"--- 4. DELETE: Видалення тестового користувача ID {test_id} ---")
        sql_delete = f"DELETE FROM {TABLE_NAME} WHERE id = %s;"
        execute_crud_operation(
            sql=sql_delete,
            params=(test_id,),
            operation_name="DELETE"
        )

        # 4.1. SELECT: Вивід таблиці після DELETE
        conn_post_delete = get_connection()
        if conn_post_delete:
            print_table(conn_post_delete, limit=10,
                        title=f"4.1. SELECT: Стан таблиці після DELETE (видалено ID: {test_id})")
            conn_post_delete.close()