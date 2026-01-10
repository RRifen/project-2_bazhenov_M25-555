from prettytable import PrettyTable

from src.primitive_db.utils import load_table_data, save_table_data

ALLOWED_COLUMNS_TYPES = ("int", "str", "bool")


def create_table(metadata, table_name, columns):
    """Создает новую таблицу в метаданных"""
    if table_name in metadata:
        raise ValueError(f"Таблица '{table_name}' уже существует")

    parsed_columns = []

    for col in columns:
        if ":" not in col:
            raise ValueError(
                f"Некорректный формат столбца: '{col}'. Ожидается формат 'name:type'"
            )
        col_name, col_type = map(str.strip, col.split(":", 1))

        if col_type not in ALLOWED_COLUMNS_TYPES:
            raise ValueError(
                f"Некорректный тип данных '{col_type}' в столбце '{col_name}'. "
                f"Допустимые типы: {ALLOWED_COLUMNS_TYPES}"
            )

        parsed_columns.append((col_name, col_type))

    if not any(col[0] == "ID" for col in parsed_columns):
        parsed_columns.insert(0, ("ID", "int"))
    else:
        id_index = next(i for i, col in enumerate(parsed_columns) if col[0] == "ID")
        id_column = parsed_columns.pop(id_index)
        if id_column[1] != "int":
            parsed_columns[0] = ("ID", "int")
        else:
            parsed_columns.insert(0, id_column)

    metadata[table_name] = {col[0]: col[1] for col in parsed_columns}

    return metadata


def drop_table(metadata, table_name):
    """Удаляет таблицу из метаданных"""
    if table_name not in metadata:
        raise ValueError(f"Таблица '{table_name}' не существует")

    del metadata[table_name]

    return metadata


def insert(metadata, table_name, values):
    """Вставляет новую запись в таблицу"""
    if table_name not in metadata:
        raise ValueError(f"Таблица '{table_name}' не существует")
    
    table_schema = metadata[table_name]
    columns = list(table_schema.keys())
    
    data_columns = columns[1:]
    
    if len(values) != len(data_columns):
        raise ValueError(
            f"Количество значений ({len(values)}) не соответствует "
            f"количеству столбцов ({len(data_columns)}). "
            f"Ожидается {len(data_columns)} значений"
        )
    
    converted_values = []
    for i, value_str in enumerate(values):
        col_name = data_columns[i]
        expected_type = table_schema[col_name]
        try:
            converted_value = convert_value(value_str, expected_type)
            converted_values.append(converted_value)
        except ValueError as e:
            raise ValueError(
                f"Ошибка валидации для столбца '{col_name}': {e}"
            ) from e
    
    table_data = load_table_data(table_name)
    
    if not table_data:
        table_data = []
    
    if table_data:
        existing_ids = [record["ID"] for record in table_data]
        new_id = max(existing_ids) + 1
    else:
        new_id = 1
    
    new_record = {"ID": new_id}
    for i, col_name in enumerate(data_columns):
        new_record[col_name] = converted_values[i]
    
    table_data.append(new_record)
    save_table_data(table_name, table_data)
    
    return new_id


def convert_value(value_str, expected_type):
    """Преобразует строковое значение в нужный тип"""
    value_str = value_str.strip()
    
    if expected_type == "int":
        try:
            return int(value_str)
        except ValueError:
            raise ValueError(f"Невозможно преобразовать '{value_str}' в int")
    elif expected_type == "str":
        if value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]
        else:
            raise ValueError(f"Некорректный формат строки: '{value_str}'. "
                             f"Строковые значения должны быть в кавычках")
    elif expected_type == "bool":
        value_lower = value_str.lower()
        if value_lower in ("true", "1"):
            return True
        elif value_lower in ("false", "0"):
            return False
        else:
            raise ValueError(f"Невозможно преобразовать '{value_str}' в bool")
    else:
        raise ValueError(f"Неподдерживаемый тип: {expected_type}")


def select(table_data, where_clause=None):
    """Выбирает записи из таблицы с опциональным условием WHERE"""
    if not table_data:
        return "Записей не найдено"
    
    if where_clause:
        filtered_data = []
        for record in table_data:
            match = True
            for column, value in where_clause.items():
                if record[column] != value:
                    match = False
                    break
            if match:
                filtered_data.append(record)
        data_to_display = filtered_data
    else:
        data_to_display = table_data
    
    if not data_to_display:
        return "Записей не найдено"
    
    columns = list(data_to_display[0].keys())
    table = PrettyTable(columns)
    
    for record in data_to_display:
        row = [record[col] for col in columns]
        table.add_row(row)
    
    return table.get_string()


def update(table_data, set_clause, where_clause):
    """Обновляет записи в таблице"""
    updated_records_ids = set()

    if not table_data:
        return updated_records_ids
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record[column] != value:
                match = False
                break
        
        if match:
            for column, value in set_clause.items():
                record[column] = value
            updated_records_ids.add(record["ID"])
    
    return updated_records_ids


def delete(table_data, where_clause):
    """Удаляет записи из таблицы"""
    deleted_records_ids = set()

    if not table_data:
        return deleted_records_ids
    
    records_to_keep = []
    
    for record in table_data:
        match = True
        for column, value in where_clause.items():
            if record[column] != value:
                match = False
                break
        
        if match:
            deleted_records_ids.add(record["ID"])
        else:
            records_to_keep.append(record)
    
    table_data.clear()
    table_data.extend(records_to_keep)
    
    return deleted_records_ids


def info(metadata, table_name):
    """Выводит информацию о таблице"""
    if table_name not in metadata:
        raise ValueError(f"Таблица '{table_name}' не существует")
    
    table_schema = metadata[table_name]
    
    columns_info = []
    for col_name, col_type in table_schema.items():
        columns_info.append(f"{col_name}:{col_type}")
    columns_str = ", ".join(columns_info)
    
    table_data = load_table_data(table_name)
    if not table_data:
        record_count = 0
    else:
        record_count = len(table_data)
    
    result = f"Таблица: {table_name}\n"
    result += f"Столбцы: {columns_str}\n"
    result += f"Количество записей: {record_count}"
    
    return result
