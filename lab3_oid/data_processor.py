import json
import logging
from typing import Dict, Any, Set, Tuple

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Конфігурація для валідації (тиск у діапазоні 0 до 50)
VALIDATION_CONFIG = {
    "value_min": 0.0,
    "value_max": 50.0
}


def validate_record(record: Dict[str, Any]) -> Tuple[bool, str]:
    """Виконує валідацію схеми та діапазону."""

    # 1. Перевірка схеми (наявність полів)
    required_fields = ["sensor_id", "timestamp", "sequence_id", "value"]
    for field in required_fields:
        if field not in record:
            return False, f"MISSING_FIELD: Поле '{field}' відсутнє."

    # 2. Перевірка типу даних
    try:
        value = float(record["value"])
    except (ValueError, TypeError):
        return False, "FORMAT_ERROR: 'value' не є числовим типом."

    # 3. Перевірка діапазону
    if not (VALIDATION_CONFIG["value_min"] <= value <= VALIDATION_CONFIG["value_max"]):
        return False, f"RANGE_ERROR: Значення {value} поза допустимим діапазоном ({VALIDATION_CONFIG['value_min']}-{VALIDATION_CONFIG['value_max']})."

    return True, "Valid"


def process_data_stream(stream_path: str, clean_data_path: str, dlq_path: str, show_detailed_logs: bool = False) -> \
Dict[str, int]:


    # Набір для відстеження унікальних ідентифікаторів (для ІДЕМПОТЕНТНОСТІ)
    processed_ids: Set[str] = set()

    stats = {"processed": 0, "valid": 0, "duplicates": 0, "invalid": 0}

    try:
        with open(stream_path, 'r') as stream_file, \
                open(clean_data_path, 'w') as clean_file, \
                open(dlq_path, 'w') as dlq_file:

            if show_detailed_logs:
                print("\n--- ЕТАП 2: ОБРОБКА ТА ВАЛІДАЦІЯ (Детальні Логи) ---")

            for line in stream_file:
                stats["processed"] += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    error_message = "JSON_DECODE_ERROR: Неможливо розібрати JSON."
                    dlq_file.write(json.dumps({"raw_data": line, "error": error_message}) + "\n")
                    if show_detailed_logs: logging.warning(f"[{stats['processed']}] DLQ: {error_message}")
                    stats["invalid"] += 1
                    continue

                # Запобігання помилці, якщо відсутні ключові поля для ID
                if not all(k in record for k in ['sensor_id', 'sequence_id']):
                    unique_id = f"UNKNOWN_{stats['processed']}"
                else:
                    unique_id = f"{record['sensor_id']}_{record['sequence_id']}"

                # 1. Перевірка на дублікат (Ідемпотентність)
                if unique_id in processed_ids:
                    if show_detailed_logs: logging.info(
                        f"[{stats['processed']}] DUPLICATE (ІДЕМПОТЕНТНІСТЬ): Виявлено дублікат {unique_id}. Пропущено.")
                    stats["duplicates"] += 1
                    continue

                # 2. Валідація даних
                is_valid, error_reason = validate_record(record)

                if is_valid:
                    # Валідний запис: зберігаємо, відзначаємо ID як оброблений
                    clean_file.write(json.dumps(record) + "\n")
                    processed_ids.add(unique_id)
                    # if show_detailed_logs: logging.debug(f"[{stats['processed']}] VALID: Оброблено {unique_id}.")
                    stats["valid"] += 1
                else:
                    # Невалідний запис: відправляємо в DLQ (Мертва черга)
                    dlq_record = record.copy()
                    dlq_record["error"] = error_reason
                    dlq_file.write(json.dumps(dlq_record) + "\n")
                    if show_detailed_logs: logging.warning(
                        f"[{stats['processed']}] DLQ (ПОМИЛКА): Запис {unique_id} - {error_reason}")
                    stats["invalid"] += 1

            if show_detailed_logs:
                print("--- Споживач: Обробка завершена. ---")
            return stats

    except FileNotFoundError:
        logging.error(f"Файл не знайдено: {stream_path}")
        return stats

