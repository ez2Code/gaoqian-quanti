import datetime


def from_str(time_str, time_format="%Y-%m-%d"):
    return datetime.datetime.strptime(time_str, time_format).date()


def plus_days(time, delta):
    return time + datetime.timedelta(days=delta)


def get_today() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d")
