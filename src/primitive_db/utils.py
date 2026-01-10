import json
import os

DATA_FOLDER_PATH = "./data"
DB_META_FILEPATH = f"{DATA_FOLDER_PATH}/db_meta.json"


def load_metadata():
    """Загружает метаданные из JSON-файла"""
    try:
        with open(DB_META_FILEPATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data):
    """Сохраняет переданные метаданные в JSON-файл"""
    with open(DB_META_FILEPATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_table_data(table_name):
    """Загружает данные таблицы из JSON-файла"""
    try:
        with open(f"{DATA_FOLDER_PATH}/{table_name}.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name, data):
    """Сохраняет переданные данные таблицы в JSON-файл"""
    with open(f"{DATA_FOLDER_PATH}/{table_name}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def clear_table_data(table_name):
    table_path = f"{DATA_FOLDER_PATH}/{table_name}.json"
    if os.path.exists(table_path):
        os.remove(table_path)
