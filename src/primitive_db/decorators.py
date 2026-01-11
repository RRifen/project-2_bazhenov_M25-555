import time
from functools import wraps

from src.primitive_db.constants import ACTION_SKIP_FLAG


def handle_db_errors(func):
    """Декоратор для обработки ошибок базы данных"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (KeyError, ValueError, FileNotFoundError) as e:
            print(e)
            return ACTION_SKIP_FLAG
    return wrapper


def confirm_action(action_name):
    """Декоратор для запроса подтверждения перед выполнением опасных операций"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            response = input(f'Вы уверены, что хотите выполнить "{action_name}"? ' 
                             '[y/n]: ')
            if response.lower() != 'y':
                print("Операция отменена")
                return ACTION_SKIP_FLAG
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """Декоратор для логирования времени выполнения функции"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.monotonic()
        result = func(*args, **kwargs)
        end_time = time.monotonic()
        elapsed_time = end_time - start_time
        print(f"Функция {func.__name__} выполнилась за {elapsed_time:.3f} секунд.")
        return result
    return wrapper


def create_cacher():
    """Создает функцию кэширования результатов вычислений"""
    cache = {}
    
    def cache_result(key, value_func):
        """Кэширует результат выполнения функции value_func по ключу key"""
        if key in cache:
            return cache[key]
        result = value_func()
        cache[key] = result
        return result
    
    return cache_result
