import baostock as bs
import logging

field_mapping = {
    "adjustflag": "adjust_flag",
    "pctChg": "percent",
    "code": "symbol"
}


def get_market(symbol: str, start_date: str, end_date: str):
    client = bs.login()
    assert client.error_code == '0'
    logging.info('login success!')

    rs = bs.query_history_k_data_plus(symbol,
                                      "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,pctChg",
                                      start_date=start_date, end_date=end_date, frequency="d")
    bs.logout()
    assert rs.error_code == '0'
    logging.info("query data success!")
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = []
    for row in data_list:
        packet = {}
        for _ in range(len(rs.fields)):
            filed = rs.fields[_]
            packet[field_mapping.get(filed, filed)] = row[_]
        fix_data(packet)
        result.append(packet)
    return result


def fix_data(packet):
    if not packet.get("turn"):
        packet["turn"] = 0
