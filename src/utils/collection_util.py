def trans_to_dict(source_data: list, key: str):
    result = dict()
    for item in source_data:
        result[item.get(key)] = item
    return result


def get_max(data_list):
    result = data_list[0]
    for item in data_list:
        result = item if item > result else result
    return result
