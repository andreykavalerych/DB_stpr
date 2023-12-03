import os
import psycopg2

# Параметры подключения к базе данных
# DB_HOST = 'your_host'
# DB_NAME = 'your_dbname'
# DB_USER = 'your_user'
# DB_PASS = 'your_password'

DB_HOST = '192.168.63.134'
DB_NAME = 'mydbname'
DB_USER = 'postgres'
DB_PASS = '123321'

tableDIR = "tableindex_PostgreSQL" # папка для сохранения процедур


# Устанавливаем соединение
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
cursor = conn.cursor()

# Получение списка всех таблиц
cursor.execute("SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
tables = cursor.fetchall()

# Функция для получения CREATE TABLE скрипта
def get_create_table_script(db_name, schema, table_name):
    create_table_script = f"CREATE TABLE {schema}.{table_name} (\n"

    # Получение информации о столбцах
    columns_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
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

for table in tables:
    db_name = table[0]
    schema_name = table[1]
    table_name = table[2]
    create_table_sql = get_create_table_script(db_name, schema_name, table_name)

    # Запись SQL-скрипта в файл
    with open(f"{tableDIR}/{db_name}_{schema_name}_{table_name}_create.sql", "w") as file:
        file.write(create_table_sql)

cursor.close()
conn.close()
