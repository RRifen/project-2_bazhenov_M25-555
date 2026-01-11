import re
import shlex

from prompt import string

from src.primitive_db.constants import (
    DELETE_PATTERN,
    INSERT_PATTERN,
    INSERT_VALUE_PATTERN,
    SELECT_PATTERN,
    UPDATE_PATTERN,
)
from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    info,
    insert,
    select,
    update,
)
from src.primitive_db.parser import parse_set, parse_where
from src.primitive_db.utils import (
    clear_table_data,
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def run():
    """Главная функция с основным циклом программы"""
    print("Первая попытка запустить проект!")
    print("***")

    print_help()

    while True:
        metadata = load_metadata()

        user_input = string("Введите команду: ").strip()

        args = shlex.split(user_input)

        if not args:
            continue

        command = args[0]

        match command:
            case "exit":
                break
            case "help":
                print_help()
            case "create_table":
                if len(args) < 3:
                    print("Ошибка: недостаточно аргументов для create_table")
                    print(
                        "Использование: create_table <имя_таблицы> <столбец1:тип> ..."
                    )
                    continue
                table_name = args[1]
                columns = args[2:]
                metadata = create_table(metadata, table_name, columns)
                if is_result_should_be_skipped(metadata):
                    continue

                save_metadata(metadata)
                save_table_data(table_name, [])
                print(f"Таблица '{table_name}' успешно создана")
            case "drop_table":
                if len(args) < 2:
                    print("Ошибка: недостаточно аргументов для drop_table")
                    print("Использование: drop_table <имя_таблицы>")
                    continue
                table_name = args[1]
                metadata = drop_table(metadata, table_name)
                if is_result_should_be_skipped(metadata):
                    continue

                save_metadata(metadata)
                clear_table_data(table_name)
                print(f"Таблица '{table_name}' успешно удалена")
            case "list_tables":
                if not metadata:
                    print("Нет созданных таблиц")
                else:
                    print("Список таблиц:")
                    for table_name in metadata.keys():
                        print(f"  - {table_name}")
            case "insert":
                match = re.search(INSERT_PATTERN, user_input)
                
                if not match:
                    print("Ошибка: некорректный формат команды insert")
                    print("Использование: insert into <имя_таблицы> " \
                        "values (<значение1>, <значение2>, ...)")
                    continue
                
                table_name = match.group(1)
                values_str = match.group(2).strip()
                
                if not values_str:
                    print("Ошибка: не указаны значения для вставки")
                    continue
                
                value_matches = re.findall(INSERT_VALUE_PATTERN, values_str)

                values = [v.strip() for v in value_matches if v.strip()]
                
                if not values:
                    print("Ошибка: не указаны значения для вставки")
                    continue
                
                new_id = insert(metadata, table_name, values)
                if is_result_should_be_skipped(new_id):
                    continue

                print(f'Запись с ID={new_id} успешно '
                        f'добавлена в таблицу "{table_name}".')
            case "info":
                if len(args) < 2:
                    print("Ошибка: недостаточно аргументов для info")
                    print("Использование: info <имя_таблицы>")
                    continue
                table_name = args[1]
                info_text = info(metadata, table_name)
                if is_result_should_be_skipped(info_text):
                    continue

                print(info_text)
            case "select":
                match = re.search(SELECT_PATTERN, user_input)
                
                if not match:
                    print("Ошибка: некорректный формат команды select")
                    print("Использование: select from <имя_таблицы> "
                        "[where <столбец> = <значение>]")
                    continue
                
                table_name = match.group(1)
                where_str = match.group(2).strip() if match.group(2) else None
                
                if table_name not in metadata:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                table_schema = metadata[table_name]
                table_data = load_table_data(table_name)
                
                where_clause = parse_where(where_str, table_schema)
                if is_result_should_be_skipped(where_clause):
                    continue
                
                result = select(table_data, where_clause)
                if is_result_should_be_skipped(result):
                    continue
                print(result)
            case "update":
                match = re.search(UPDATE_PATTERN, user_input)
                
                if not match:
                    print("Ошибка: некорректный формат команды update")
                    print("Использование: update <имя_таблицы> " \
                        "set <столбец> = <значение> where <столбец> = <значение>")
                    continue
                
                table_name = match.group(1)
                set_str = match.group(2).strip()
                where_str = match.group(3).strip()
                
                if table_name not in metadata:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                table_schema = metadata[table_name]
                table_data = load_table_data(table_name)
                
                set_clause = parse_set(set_str, table_schema)
                where_clause = parse_where(where_str, table_schema)

                if (is_result_should_be_skipped(set_clause) or 
                    is_result_should_be_skipped(where_clause)):
                    continue
                
                updated_records_ids = update(table_data, set_clause, where_clause)
                if is_result_should_be_skipped(updated_records_ids):
                    continue
                
                if updated_records_ids:
                    save_table_data(table_name, table_data)
                
                if len(updated_records_ids) > 1:
                    print(f"Записи с ID={updated_records_ids} в таблице "
                            f"{table_name} успешно обновлены")
                elif len(updated_records_ids) == 1:
                    print(f"Запись с ID={list(updated_records_ids)[0]} "
                            f"в таблице {table_name} успешно обновлена")
                else:
                    print("Ни одна запись не была обновлена")
            case "delete":
                match = re.search(DELETE_PATTERN, user_input)
                
                if not match:
                    print("Ошибка: некорректный формат команды delete")
                    print("Использование: delete from <имя_таблицы> " \
                        "where <столбец> = <значение>")
                    continue
                
                table_name = match.group(1)
                where_str = match.group(2).strip()
                
                if table_name not in metadata:
                    print(f"Ошибка: Таблица '{table_name}' не существует")
                    continue
                
                table_schema = metadata[table_name]
                table_data = load_table_data(table_name)
                
                where_clause = parse_where(where_str, table_schema)
                if is_result_should_be_skipped(where_clause):
                    continue
                
                deleted_records_ids = delete(table_data, where_clause)
                if is_result_should_be_skipped(deleted_records_ids):
                    continue
                
                if deleted_records_ids:
                    save_table_data(table_name, table_data)
                
                if len(deleted_records_ids) > 1:
                    print(f"Записи с ID={deleted_records_ids} "
                            f"успешно удалены из таблицы {table_name}")
                elif len(deleted_records_ids) == 1:
                    print(f"Запись с ID={list(deleted_records_ids)[0]} "
                            f"успешно удалена из таблицы {table_name}")
                else:
                    print("Ни одна запись не была удалена")
            case _:
                print(f"Неизвестная команда: {command}")
                print("Введите 'help' для справки")


def print_help():
    """Печатает информацию по доступным командам"""

    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> info <имя_таблицы> - вывести информацию о таблице")

    print("\n***Операции с данными***")
    print("Функции:")
    print("<command> insert into <имя_таблицы> " \
        "values (<значение1>, <значение2>, ...) - создать запись")
    print("<command> select from <имя_таблицы> " \
        "where <столбец> = <значение> - прочитать записи по условию")
    print("<command> select from <имя_таблицы> - прочитать все записи")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> " \
        "where <столбец_условия> = <значение_условия> - обновить запись")
    print("<command> delete from <имя_таблицы> where "
        "<столбец_условия> = <значение_условия> - удалить запись")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")


def is_result_should_be_skipped(result):
    """Проверяет, нужно ли пропустить результат выполнения команды"""
    if result == 'ACTION_SKIP_FLAG':
        return True
    return False
