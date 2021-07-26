from datetime import datetime

def log_time(func):
    def wrapped(*args, **kwargs):
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        print(f'==========> Running: {func.__name__} Start at {current_time}  <==========')
        result = func(*args, **kwargs)
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        print(f'==========> Running: {func.__name__} End at {current_time}  <========== ')
        return result
    return wrapped
