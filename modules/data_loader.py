import pandas as pd
import mysql.connector
import re


database = mysql.connector.connect(host="192.168.1.197", user="nibinpsan", passwd="dhanya123")
cursor = database.cursor()

df = pd.read_excel("D:\\code\\zack.xlsx")
f = open("D:\\code\\test.txt", "w")
industry_dictionary = {}
myList = df['Industry'].unique().tolist()
my_list = list(df.columns.values)

error_count = 0
column_list = {}
reference_name = {}
counter = 0
query = "select * from reference.REFERENCE_TABLE"
table_name = "zacks_database.zacks_table"
truncate_table_stmt = "TRUNCATE TABLE " + table_name
drop_table_stmt = "DROP TABLE " + table_name
create_table_string = "CREATE TABLE " + table_name + " ("
insert_data_query = "insert into " + table_name + " ("
# get reference name from DB for TABLE CREATION

# This section checks existance of database and create it if it does not exist.

create_database_statement = "show databases like 'zacks_database'"
cursor.execute(create_database_statement)
database_check = cursor.fetchone()
if str(database_check) == "None":
    try:
        cursor.execute("create database zacks_database")
        database.commit()
    except mysql.connector.Error as error:
        print("Failed creating database {}" + format(error))
        f.write(print("Failed creating database {}" + str(error)) + "\n")
        f.write("create database zacks_database\n")
        print("****Query attempted!!")
        print("create database zacks_database\n")
        database.rollback()
        error_count += 1

# This section of the code is for trucating table and the dropping it.
# New table will be created for new data

cursor.execute(query)
j = 0
for i in cursor.fetchall():
    reference_name[str(i[0])] = str(i[1])

stmt = "SELECT COUNT(*) \
        FROM information_schema.tables \
        WHERE table_name = 'zacks_table'"
cursor.execute(stmt)
result = cursor.fetchone()

print(str(result))

if result[0] != 0:
    try:
        # cursor.execute(truncate_table_stmt)
        cursor.execute(drop_table_stmt)
        database.commit()
    except mysql.connector.Error as error:
        print("Failed TRUNCATING AND DROPPING {}" + format(error))
        f.write(print("Failed TRUNCATING AND DROPPING {}" + str(error)) + "\n")
        f.write(truncate_table_stmt+"\n")
        print("****Query attempted!!")
        print(truncate_table_stmt)
        database.rollback()
        error_count += 1
# else:
#    print("****TABLE " + table_name + " DOESNT EXIST******\n")

# This section of the code creates new table

match_counter = 0

for each_val in my_list:
    column_name = re.sub('[^A-Za-z0-9]+', '', each_val)
    for key, value in reference_name.items():
        # print("TRYING TO MATCH " + key.lower() + " COLUMN NAME " + column_name.lower())
        if re.search(key.lower(), column_name.lower()):
            # print("*****MATCHED " + key + " found in " + column_name + " column******")
            counter += 1
            create_table_string += "" + column_name + " " + str(value) + ","
            insert_data_query += column_name + ","
            match_counter += 1
        if match_counter == 0:
            create_table_string += "" + column_name + " " + " FLOAT(10,2),"
            insert_data_query += column_name + ","

    # print("\n\n\n")
insert_data_query = insert_data_query[:-1]
insert_data_query += ") VALUES ("
create_table_string += " PRIMARY KEY (Ticker));"
print("STRING MATCHES :" + str(counter) + "\n")
# print(create_table_string)

try:
    cursor.execute(create_table_string)
    database.commit()
except mysql.connector.Error as error:
    print("Failed to create table {" + table_name + "}" + format(error))
    f.write(print("Failed to create table {" + table_name + "}" + str(error)) + "\n")
    f.write("****Query attempted!!\n")
    f.write(create_table_string+"\n")
    print("****Query attempted!!")
    print(create_table_string)
    database.rollback()
    error_count += 1

# This section of the code ADDs new records into the stable

i = []
j = 0
error_count = 0

for index, row in df.iterrows():
    i = 0
    local_query = insert_data_query
    while i < len(my_list):
        if isinstance(row[my_list[i]], (float, int)):
            # print(str(row[my_list[i]]) + " is a number \n")
            local_query += "\"" + str(round(row[my_list[i]], 2)) + "\","
        else:
            # print(str(row[my_list[i]]) + " is a STRING \n")
            local_query += "\"" + str(row[my_list[i]]) + "\","
        i += 1

    local_query = local_query[:-1]
    local_query += ")"
    # print("LOCAL QUERY CREATED FOR INSERT:\n" + local_query)

    try:
        cursor.execute(local_query)
        database.commit()
    except mysql.connector.Error as error:
        print("Failed inserting record into " + table_name + " table {}" + format(error))
        f.write(print("Failed inserting record into " + table_name + " table {}" + str(error)) + "\n")
        f.write("****Query attempted!!\n")
        f.write(local_query+"\n")
        print(local_query)
        print("****Query attempted!!")
        database.rollback()
        error_count += 1

f.close()
database.commit()
cursor.close()
database.close()
