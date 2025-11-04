import psycopg2
from faker import Faker
import random

DB_CONFIG = {
    "dbname": "oid_lab0",
    "user": "postgres",        # <-- ЗАМІНІТЬ! Наприклад, "postgres"
    "password": "AB87povb_09s",  # <-- ЗАМІНІТЬ!
    "host": "localhost",
    "port": "5432"
}

NUM_RECORDS = 100
fake = Faker('uk_UA')  # 'uk_UA' - для генерації даних українською, або 'en_US' для англійської


def generate_fake_data(num_records):
    """Генерує список кортежів із фейковими даними."""
    fake_users = []
    for _ in range(num_records):
        # 1. Створення імені користувача (username)
        # Комбінуємо унікальне ім'я та випадкове число
        name_part = fake.user_name()[:20]
        unique_username = f"{name_part}{random.randint(100, 999)}"

        # 2. Створення електронної пошти (email)
        # Використовуємо функцію email, оскільки вона гарантує валідний формат
        email = fake.email()

        # 3. Створення віку (age)
        age = random.randint(18, 65)

        # Додаємо кортеж даних до списку
        fake_users.append((unique_username, email, age))

    return fake_users


def insert_data_to_db(data):
    """Підключається до БД і вставляє дані пакетно."""
    conn = None
    try:
        # 1. Підключення до бази даних
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 2. SQL-запит для вставки
        # Використовуємо %s як плейсхолдер
        insert_query = """
            INSERT INTO users (username, email, age) 
            VALUES (%s, %s, %s)
            ON CONFLICT (username) DO NOTHING;
        """

        # 3. Пакетна вставка
        # Цей метод значно швидший за цикл з окремими INSERT
        cur.executemany(insert_query, data)
        conn.commit()

        print(f" Успішно додано {cur.rowcount} записів до таблиці 'users'.")

        cur.close()

    except psycopg2.Error as e:
        print(f" Помилка при роботі з PostgreSQL: {e}")
        if conn:
            conn.rollback()  # Відкотити транзакцію у разі помилки

    finally:
        if conn:
            conn.close()


# --- Головна функція запуску ---
if __name__ == '__main__':
    print(f"Генерація {NUM_RECORDS} тестових записів...")
    new_users_data = generate_fake_data(NUM_RECORDS)

    print("Завантаження даних у PostgreSQL...")
    insert_data_to_db(new_users_data)

    print("Завершено.")