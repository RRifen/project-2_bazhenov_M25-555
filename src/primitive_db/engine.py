import shlex

from prompt import string

from src.primitive_db.core import create_table, drop_table
from src.primitive_db.utils import load_metadata, save_metadata

DB_META_FILEPATH = "./db_meta.json"


def run():
    """Главная функция с основным циклом программы"""
    print("Первая попытка запустить проект!")
    print("***")

    print_help()

    while True:
        # Загружаем актуальные метаданные
        metadata = load_metadata(DB_META_FILEPATH)

        # Запрашиваем ввод у пользователя
        user_input = string("Введите команду: ").strip()

        # Разбираем введенную строку на команду и аргументы
        args = shlex.split(user_input)

        if not args:
            continue

        command = args[0]

        # Обрабатываем команды
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
                try:
                    table_name = args[1]
                    columns = args[2:]
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(DB_META_FILEPATH, metadata)
                    print(f"Таблица '{table_name}' успешно создана")
                except ValueError as e:
                    print(f"Ошибка: {e}")
            case "drop_table":
                if len(args) < 2:
                    print("Ошибка: недостаточно аргументов для drop_table")
                    print("Использование: drop_table <имя_таблицы>")
                    continue
                try:
                    table_name = args[1]
                    metadata = drop_table(metadata, table_name)
                    save_metadata(DB_META_FILEPATH, metadata)
                    print(f"Таблица '{table_name}' успешно удалена")
                except ValueError as e:
                    print(f"Ошибка: {e}")
            case "list_tables":
                if not metadata:
                    print("Нет созданных таблиц")
                else:
                    print("Список таблиц:")
                    for table_name in metadata.keys():
                        print(f"  - {table_name}")
            case _:
                print(f"Неизвестная команда: {command}")
                print("Введите 'help' для справки")


def print_help():
    """Prints the help message for the current mode"""

    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")
