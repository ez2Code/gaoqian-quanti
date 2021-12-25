from utils import database
from utils import baostock_api
from utils import date_util
from utils import collection_util
from utils import common_util
from functools import wraps
import os
import fcntl


def exec_with_file_lock(function):
    @wraps(function)
    def decorated(*args, **kwargs):
        with open(os.path.abspath(__file__), 'r') as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX)
                return function(*args, **kwargs)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    return decorated


@exec_with_file_lock
def grab_daily():
    symbols = database.select(table="tbl_symbol")
    for s in symbols:
        handle_one_symbol(s)


def query_last_year_data(symbol, current_date):
    """
    :param symbol:
    :param current_date: 表示数据库中记录的最后抓取日期，本次的开始日期
    :return:
    """
    season_start = date_util.plus_days(date_util.from_str(current_date), -380).strftime("%Y-%m-%d")
    rows = database.select("tbl_market", equal={"symbol": symbol}, less_than={"date": current_date},
                           greater_than={"date": season_start})
    return collection_util.trans_to_dict(rows, "date")


def handle_one_symbol(symbol):
    data_cache = query_last_year_data(symbol.get("symbol_id"), symbol.get("grabbed_date"))
    today = date_util.get_today()
    start_date = symbol.get("grabbed_date")
    grab_end = start_date

    def get_cache_data(source_date, extra_day=0):
        """
        获取缓存的数据，用于计算差值，由于目标日期可能并非交易日，所以默认往前推一天，最多推10天
        :param source_date:
        :param extra_day:
        :return:
        """
        cache_date = source_date
        if extra_day > 10:
            return None
        if extra_day > 0:
            cache_date = date_util.plus_days(date_util.from_str(source_date), -extra_day).strftime("%Y-%m-%d")
        return data_cache.get(cache_date) if cache_date in data_cache else get_cache_data(source_date, extra_day + 1)

    while grab_end < today:
        start_date = grab_end
        grab_end = date_util.plus_days(date_util.from_str(start_date), 30).strftime("%Y-%m-%d")
        grabbed = baostock_api.get_market(symbol.get("symbol_id"), start_date, grab_end)
        # 保存到缓存，用于后续日期计算涨幅
        data_cache.update(collection_util.trans_to_dict(grabbed, "date"))
        for item in grabbed:
            week_date = date_util.plus_days(date_util.from_str(item.get("date")), -7).strftime("%Y-%m-%d")
            week_record = get_cache_data(week_date)
            if week_record:
                item["percent_week"] = common_util.calc_percent(item.get("close"), week_record.get("close"))
            month_date = date_util.plus_days(date_util.from_str(item.get("date")), -30).strftime("%Y-%m-%d")
            month_record = get_cache_data(month_date)
            if month_record:
                item["percent_month"] = common_util.calc_percent(item.get("close"), month_record.get("close"))
            season_date = date_util.plus_days(date_util.from_str(item.get("date")), -90).strftime("%Y-%m-%d")
            season_record = get_cache_data(season_date)
            if season_record:
                item["percent_season"] = common_util.calc_percent(item.get("close"), season_record.get("close"))
            database.insert("tbl_market", values=item)
            half_annual_date = date_util.plus_days(date_util.from_str(item.get("date")), -180).strftime("%Y-%m-%d")
            half_annual_record = get_cache_data(half_annual_date)
            if half_annual_record:
                item["percent_half_annual"] = common_util.calc_percent(
                    item.get("close"), half_annual_record.get("close"))
            annual_date = date_util.plus_days(date_util.from_str(item.get("date")), -365).strftime("%Y-%m-%d")
            annual_record = get_cache_data(annual_date)
            if annual_record:
                item["percent_annual"] = common_util.calc_percent(
                    item.get("close"), annual_record.get("close"))
            database.insert("tbl_market", values=item)
        latest_date = collection_util.get_max(list(data_cache.keys()))
        database.update("tbl_symbol", fields={"grabbed_date": latest_date},
                        equal={"symbol_id": symbol.get("symbol_id")})


grab_daily()
