from prompt import string


def welcome():
    """Функция приветствия и игрового цикла с парсингом команд."""
    print("Первая попытка запустить проект!")
    print("***")

    help()

    while True:
        command = string("Введите команду: ").strip()

        if command == "exit":
            break
        elif command == "help":
            help()
        else:
            continue


def help():
    print("<command> exit - выйти из программы")
    print("<command> help - справочная информация")
