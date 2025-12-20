import os
import json
from data_generator import generate_sensor_data
from data_processor import process_data_stream

# Визначення шляхів до файлів
INPUT_STREAM_FILE = "input_stream.txt"
CLEAN_DATA_FILE = "clean_data.jsonl"
DLQ_FILE = "dead_letter_queue.jsonl"


def display_raw_stream():
    """Виводить згенерований сирий потік у консоль (УСІ РЯДКИ)."""
    if not os.path.exists(INPUT_STREAM_FILE):
        print(f"Помилка: Файл {INPUT_STREAM_FILE} не знайдено.")
        return

    print("\n### 📋 ЗГЕНЕРОВАНИЙ СИРИЙ ПОТІК (Вхідні дані) ###")
    with open(INPUT_STREAM_FILE, "r") as f:
        lines = f.readlines()

        print(f"Кількість записів у потоці: {len(lines)}\n")

        # *** ЗМІНА ТУТ: ВИДАЛЯЄМО ОБМЕЖЕННЯ [ :15 ] ***
        for i, line in enumerate(lines):  # Перебираємо УСІ рядки
            line = line.strip()
            if not line: continue

            suffix = ""

            # (Логіка виявлення помилок/дублікатів залишається для візуалізації)
            if "RANGE_ERROR" in line or "FORMAT_ERROR" in line or "MISSING_FIELD" in line or "ERROR_VAL" in line:
                suffix = " <-- ЙМОВІРНЕ ПОШКОДЖЕННЯ"

            # Спроба виявити простий дублікат
            if i > 0 and lines[i - 1].strip() == line:
                suffix = " <-- ДУБЛІКАТ"

            print(f"{i + 1:03}: {line}{suffix}")

        print("\n--------------------------------------------------------------")


# pipeline_runner.py (оновлена функція)

def display_dlq_content():
    """Виводить увесь вміст DLQ для демонстрації обробки помилок."""
    if not os.path.exists(DLQ_FILE) or os.path.getsize(DLQ_FILE) == 0:
        print("\n--- DLQ: Файл порожній. Всі дані були валідними. ---")
        return

    print("\n### 🚨 ВМІСТ DEAD-LETTER QUEUE (DLQ) - ПОВНИЙ ВИВІД ###")
    print(" (Записи, ізольовані через порушення цілісності)")

    dlq_records = []
    try:
        with open(DLQ_FILE, "r") as f:
            # Зчитуємо всі записи
            for line in f:
                dlq_records.append(line)
    except Exception as e:
        print(f"Помилка читання DLQ файлу: {e}")
        return

    # Перебираємо всі записи без обмежень
    for i, line in enumerate(dlq_records):
        try:
            record = json.loads(line)
            error = record.get("error", "Невідома помилка")
            seq_id = record.get("sequence_id", "N/A")

            # Виводимо запис у потрібному форматі
            print(f"* Запис ID {seq_id}: [{error.split(':')[0]}] Причина: {error}")
        except json.JSONDecodeError:
            print(f"* Помилка: Некоректний JSON у DLQ для рядка {i + 1}.")

    print(f"\nВСЬОГО записів у DLQ: {len(dlq_records)}")
    print("--------------------------------------------------------------")


def run_pipeline():
    print("==============================================")
    print("🚀 СТІЙКИЙ КОНВЕЙЄР ДАНИХ (ЛАБОРАТОРНА РОБОТА)")
    print("==============================================")

    # 1. Етап: Генерація Ненадійних Даних
    print("\n--- ЕТАП 1/3: ГЕНЕРАЦІЯ ТА СИРИЙ ВВІД ---")
    data_stream = generate_sensor_data(
        sensor_id="LAB_PRES_02",
        sequence_start=1000,
        num_records=50
    )

    # Запис у "вхідну чергу" (файл)
    with open(INPUT_STREAM_FILE, "w") as f:
        for record_str in data_stream:
            f.write(record_str + "\n")

    # Виводимо згенерований потік
    display_raw_stream()

    # 2. Етап: Обробка та Валідація
    # Очищуємо файли перед обробкою
    for f in [CLEAN_DATA_FILE, DLQ_FILE]:
        if os.path.exists(f): os.remove(f)

    print("\n--- ЕТАП 2/3: ОБРОБКА ТА ВАЛІДАЦІЯ (Логування Помилок) ---")
    # Запускаємо обробник з детальними логами
    stats = process_data_stream(INPUT_STREAM_FILE, CLEAN_DATA_FILE, DLQ_FILE, show_detailed_logs=True)

    # Виводимо вміст DLQ
    display_dlq_content()

    # 3. Етап: Підведення Підсумків
    print("\n--- ЕТАП 3/3: ФІНАЛЬНІ ПІДСУМКИ ---")

    print("### 📊 СТАТИСТИКА РОБОТИ КОНВЕЄРА ###")
    print(f"📋 Загалом оброблено записів: {stats['processed']}")
    print(f"   -> ✅ Валідних та унікальних: {stats['valid']} (Збережено в clean_data.jsonl)")
    print(f"   -> 🔄 Пропущено дублікатів: {stats['duplicates']} (Завдяки ІДЕМПОТЕНТНОСТІ)")
    print(f"   -> 🚨 Відправлено в DLQ: {stats['invalid']} (Завдяки СТІЙКОСТІ)")

    print("\nКОНВЕЄР ЗАВЕРШИВ РОБОТУ.")


if __name__ == '__main__':
    run_pipeline()
