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

# Подключение к БД
connection = pyodbc.connect(f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}')
badcopySP_helptext=list()
badcopySQL_modules=list()
countCOPY=0
countDB=0
cursor = connection.cursor()
storprocDIR = "stored_procedureMSSQL" #папка для сохранения процедур
# Получение списка хранимых процедур
cursor.execute(f"SELECT count(name) FROM {DATABASE}.sys.procedures where type_desc='SQL_STORED_PROCEDURE'")
countDB=cursor.fetchone() #cursor.fetchone(): Извлекает одну запись (строку) из результата запроса.
countDB=countDB[0]
cursor.execute(f"SELECT name FROM {DATABASE}.sys.procedures where type_desc='SQL_STORED_PROCEDURE'")
procedures = [row[0] for row in cursor.fetchall()]

if not os.path.exists(storprocDIR):
 os.mkdir(storprocDIR) # создание каталога если его нет

for proc_name in procedures:
 # Получение тела хранимой процедуры
   try:
    cursor.execute(f"EXEC sp_helptext '{proc_name}'")
    proc_body_rows = cursor.fetchall() #cursor.fetchall(): Извлекает все оставшиеся строки из результата запроса.
    # Объединение строк в одно тело процедуры
    proc_body = ' '.join(row[0] for row in proc_body_rows).strip()

    proc_body = proc_body.replace('\n', ' ')
    # Сохранение в локальный файл
    with open(f"{storprocDIR}\\{proc_name}.sql", 'w', encoding='utf-8') as file:
        file.write(proc_body)
    countCOPY+=1
   except:
    print("не смог загрузить(sp_helptext):"+proc_name)
    badcopySP_helptext.append(proc_name)
    continue

if(len(badcopySP_helptext)>0):
 print('Колл не сохранивших(sp_helptext): '+len(badcopySP_helptext).__str__())
 print('--------------')
 for proc_name2 in badcopySP_helptext:
  try:
   cursor.execute(f"SELECT m.definition FROM sys.sql_modules m INNER JOIN sys.objects o ON m.object_id = o.object_id WHERE o.name = '{proc_name2}'")
   proc_body_rows = cursor.fetchall()
   proc_body = ' '.join(row[0] for row in proc_body_rows).strip()
   proc_body = proc_body.replace('\n', ' ')
   with open(f"{storprocDIR}\\{proc_name2}.sql", 'w', encoding='utf-8') as file:
       file.write(proc_body)
   countCOPY +=  1
  except:
   print("не смог загрузить(sys.sql_modules):" + proc_name2)
   badcopySQL_modules.append(proc_name2)
   continue
print('--------------')
print('Колл не сохранивших(sp_helptext): ' + len(badcopySP_helptext).__str__())
print('Колл не сохранивших(sys.sql_modules): ' + len(badcopySQL_modules).__str__())
print('Всего процедур:  ' +countDB.__str__())
print('Процедур скачано: '+countCOPY.__str__())
cursor.close()
connection.close()
print("Процедуры успешно экспортированы!")