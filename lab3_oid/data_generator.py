import json
import time
import random
from typing import List


def generate_sensor_data(sensor_id: str, sequence_start: int, num_records: int) -> List[str]:
    """
    Генерує потік записів даних із штучно введеними помилками (пошкодження, дублікати).
    Повертає список рядків у форматі JSON.
    """
    data_stream = []

    # Стан для імітації дублювання
    last_record_str = None

    print(f"--- Генератор: Починаємо генерацію {num_records} записів для {sensor_id} ---")

    for i in range(num_records):
        sequence_id = sequence_start + i
        current_time = int(time.time())

        # Базовий валідний запис (тиск у діапазоні 20-30)
        record = {
            "sensor_id": sensor_id,
            "timestamp": current_time,
            "sequence_id": sequence_id,
            "value": round(random.uniform(20.0, 30.0), 2)
        }

        # --- Введення помилок (приблизно 20% ймовірності) ---
        if random.random() < 0.2:
            error_type = random.choice(["RANGE_ERROR", "FORMAT_ERROR", "MISSING_FIELD"])

            if error_type == "RANGE_ERROR":
                record["value"] = round(random.uniform(500.0, 5000.0), 2)  # Нереалістичне значення
                # print(f"|--- Помилка: Запис {sequence_id} - Значення поза діапазоном.")
            elif error_type == "FORMAT_ERROR":
                record["value"] = "ERROR_VAL"  # Неправильний тип даних
                # print(f"|--- Помилка: Запис {sequence_id} - Неправильний формат значення.")
            elif error_type == "MISSING_FIELD":
                del record["value"]  # Відсутнє критичне поле
                # print(f"|--- Помилка: Запис {sequence_id} - Відсутнє поле 'value'.")

        current_record_str = json.dumps(record)
        data_stream.append(current_record_str)

        # --- Дублікати (15% ймовірності) ---
        if last_record_str and random.random() < 0.15:
            # Дублюємо останній запис
            data_stream.append(last_record_str)
            # print(f"|--- Помилка: Запис {sequence_id} - Створено дублікат.")

        last_record_str = current_record_str

    print(f"--- Генератор: Генерація завершена. Згенеровано {len(data_stream)} записів. ---")
    return data_stream


if __name__ == '__main__':
    # Приклад для тестування
    stream = generate_sensor_data(sensor_id="LAB_TEST", sequence_start=1, num_records=10)
    print("\nПриклад згенерованого потоку:")
    for record_str in stream:
        print(record_str)
