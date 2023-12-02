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

# Подключение к базе данных
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
cursor = conn.cursor()
countCOPY=0
storprocDIR = "stored_procedurePostGres" #папка для сохранения процедур
# Получаем список всех хранимых процедур
cursor.execute("""
    SELECT proname FROM pg_proc p
    JOIN pg_namespace n ON (p.pronamespace = n.oid)
    WHERE n.nspname = 'public'  -- для схемы(schema) 'public' 
""")

procedures = cursor.fetchall()

if not os.path.exists(storprocDIR):
 os.mkdir(storprocDIR) # создание каталога если его нет

# Для каждой процедуры извлекаем исходный код и сохраняем в файл
for proc in procedures:
    try:
     proc_name = proc[0]
     cursor.execute(f"""
         SELECT pg_get_functiondef(p.oid) FROM pg_proc p
         JOIN pg_namespace n ON (p.pronamespace = n.oid)
         WHERE n.nspname = 'public' AND p.proname = '{proc_name}'
     """)
     code = cursor.fetchone()[0]

    # Сохраняем код в файл
     with open(f"{storprocDIR}\\{proc_name}.sql", 'w', encoding='utf-8') as file:
         file.write(code)
     countCOPY += 1
    except:
     print("не смог загрузить(pg_get_functiondef):" + proc_name)
     continue
# Закрываем соединение
cursor.close()
conn.close()
