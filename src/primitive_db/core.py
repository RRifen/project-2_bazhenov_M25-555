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
