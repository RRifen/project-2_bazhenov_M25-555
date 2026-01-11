import re

from src.primitive_db.constants import SET_CLAUSE_PATTERN, WHERE_CLAUSE_PATTERN
from src.primitive_db.core import convert_value, handle_db_errors


@handle_db_errors
def parse_where(where_str, table_schema):
    """Парсит условие WHERE и возвращает словарь вида {'column': value}"""
    if not where_str:
        return None
    
    match = re.search(WHERE_CLAUSE_PATTERN, where_str.strip())
    
    if not match:
        raise ValueError(f"Некорректный формат условия WHERE: '{where_str}'. "
                         f"Ожидается формат: <столбец> = <значение>")
    
    column_name = match.group(1).strip()
    value_str = match.group(2).strip()
    
    if column_name not in table_schema:
        raise ValueError(f"Столбец '{column_name}' не существует в таблице")
    
    column_type = table_schema[column_name]
    
    try:
        converted_value = convert_value(value_str, column_type)
    except ValueError as e:
        raise ValueError(f"Ошибка преобразования значения для столбца "
                         f"'{column_name}': {e}") from e
    
    return {column_name: converted_value}


@handle_db_errors
def parse_set(set_str, table_schema):
    """Парсит условие SET и возвращает словарь вида {'column': value}"""
    if not set_str:
        raise ValueError("Условие SET не может быть пустым")
    
    pattern = SET_CLAUSE_PATTERN
    match = re.search(pattern, set_str.strip())
    
    if not match:
        raise ValueError(f"Некорректный формат условия SET: '{set_str}'. " 
                         f"Ожидается формат: <столбец> = <значение>")
    
    column_name = match.group(1).strip()
    value_str = match.group(2).strip()
    
    if column_name not in table_schema:
        raise ValueError(f"Столбец '{column_name}' не существует в таблице")
    
    if column_name == "ID":
        raise ValueError("Нельзя изменять столбец ID")
    
    column_type = table_schema[column_name]
    
    try:
        converted_value = convert_value(value_str, column_type)
    except ValueError as e:
        raise ValueError(f"Ошибка преобразования значения для столбца "
                         f"'{column_name}': {e}") from e
    
    return {column_name: converted_value}
