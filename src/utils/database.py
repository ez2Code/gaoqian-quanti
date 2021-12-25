import mysql.connector

db_instance = mysql.connector.connect(
    host="mysql-0.mysql.mysql",
    user="username",
    passwd="password",
    port=3306,
    database="stock"
)


def select(table, fields=None, equal=None, less_than=None, greater_than=None):
    sql = f"select {','.join(fields) if fields else '*'} from {table}"
    params = []
    where = []
    if equal:
        for k, v in equal.items():
            where.append(f"{k}=%s")
            params.append(v)
    if less_than:
        for k, v in less_than.items():
            where.append(f"{k}<%s")
            params.append(v)
    if greater_than:
        for k, v in greater_than.items():
            where.append(f"{k}>%s")
            params.append(v)
    if where:
        sql += ' where ' + " and ".join(where)
    cursor = db_instance.cursor()
    cursor.execute(sql, params)
    result = []
    columns = cursor.column_names
    for row in cursor.fetchall():
        packet = {}
        for _ in range(len(columns)):
            packet[columns[_]] = row[_]
        result.append(packet)
    return result


def insert(table, values, update_on_duplicated=True):
    fields = []
    place_holders = []
    params = []
    for k, v in values.items():
        fields.append(k)
        place_holders.append("%s")
        params.append(v)
    if update_on_duplicated:
        sql = f"insert into {table}({', '.join(fields)}) values ({', '.join(place_holders)}) on duplicate key " \
              f"update {', '.join([x + '=%s' for x in fields])}"
        params.extend(params.copy())
    else:
        sql = f"insert into {table}({', '.join(fields)}) values ({', '.join(place_holders)})"
    cursor = db_instance.cursor()
    cursor.execute(sql, params)
    db_instance.commit()


def update(table, fields=None, equal=None):
    params = [x for x in fields.values()]
    where = []
    if equal:
        for k, v in equal.items():
            where.append(f"{k}=%s")
            params.append(v)
    sql = f"update {table} set {', '.join([x + '=%s' for x in fields.keys()])} where {' and '.join(where)}"
    cursor = db_instance.cursor()
    cursor.execute(sql, params)
    db_instance.commit()
