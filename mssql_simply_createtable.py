import os
import pyodbc

# Параметры подключения к БД
# SERVER = 'your_server'
# DATABASE = 'your_database'
# USERNAME = 'your_username'
# PASSWORD = 'your_password'

SERVER = '192.168.63.132'
DATABASE = 'GAGARIN'
USERNAME = 'sa'
PASSWORD = '123321'
DRIVER = '{ODBC Driver 17 for SQL Server}'  # Или другой драйвер, если вы используете другой

tableDIR = "tableindex_MSSQL" #папка для сохранения процедур
# Устанавливаем соединение
conn_str = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Получение списка всех таблиц
cursor.execute("SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = cursor.fetchall()

# Функция для получения CREATE TABLE скрипта
def get_create_table_script(db_name, schema, table_name):
    create_table_script = f"CREATE TABLE {db_name}.{schema}.{table_name} (\n"

    # Получение информации о столбцах
    columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM {db_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
        """
    cursor.execute(columns_query)
    columns = cursor.fetchall()

    for col in columns:
        col_name, col_type, col_nullable = col
        col_nullable = "NULL" if col_nullable == 'YES' else "NOT NULL"
        create_table_script += f"  {col_name} {col_type} {col_nullable},\n"

    create_table_script = create_table_script.rstrip(',\n') + "\n);"

    return create_table_script


# Перебор всех таблиц и создание SQL-скриптов
if not os.path.exists(tableDIR):
 os.mkdir(tableDIR) # создание каталога если его нет


# Перебор всех таблиц и создание SQL-скриптов
for table in tables:
    db_name = table[0]
    schema_name = table[1]
    table_name = table[2]
    create_table_sql = get_create_table_script(db_name, schema_name, table_name)

    # Запись SQL-скрипта в файл
    with open(f"{tableDIR}\\{db_name}_{schema_name}_{table_name}_create.sql", "w") as file:
        file.write(create_table_sql)

cursor.close()
conn.close()
