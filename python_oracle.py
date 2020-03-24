import cx_Oracle
import pandas as pd

def get_connection(db_name,port,service_name,user,password):
    dsn_tns = cx_Oracle.makedsn(db_name, port, service_name=service_name)
    conn = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
    return conn

def close_connection(conn):
    conn.close()
 
 
def run_query(sql,conn):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        print("Query Executed")
    except cx_Oracle.IntegrityError as e:
        errorObj, = e.args
        print("Error Code:", errorObj.code)
        print("Error Message:", errorObj.message)

 
def run_query_output(sql,conn):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        res=cursor.fetchall()
        print(res)
        return res
    except cx_Oracle.IntegrityError as e:
        errorObj, = e.args
        print("Error Code:", errorObj.code)
        print("Error Message:", errorObj.message)
 
def query_to_csv(conn,sql_query,full_path):
    cursor = conn.cursor()
    try:
        SQL_Query = pd.read_sql_query(sql_query, conn)
        df=pd.DataFrame(SQL_Query)
        df.to_csv(full_path)
    except cx_Oracle.IntegrityError as e:
        errorObj, = e.args
        print("Error Code:", errorObj.code)
        print("Error Message:", errorObj.message)
        
def query_to_df(conn,sql_query):
    cursor = conn.cursor()
    try:
        SQL_Query = pd.read_sql_query(sql_query, conn)
        df=pd.DataFrame(SQL_Query)
        return df
    except cx_Oracle.IntegrityError as e:
        errorObj, = e.args
        print("Error Code:", errorObj.code)
        print("Error Message:", errorObj.message)

 
#======================MAIN CODE=====================
 
#modify as per your convenience
#do not remove the 'r' (it ensures especial character are treated normally)
 
db_name=r''
port=r''
service_name=r''
user=r''
password=r''

# first establish the connection
conn=get_connection(db_name,port,service_name,user,password)


#========For saving Database Query as CSV
 
#provide the location to save the csv file
saving_path=r"E:"
#provide csv file's name (dont put file extension)
file_name="example"
full_path=r'{}\{}.csv'.format(saving_path,file_name)
#make sure the query returns some values
sql_query='SELECT * FROM table'
query_to_csv(conn,sql_query,full_path)

#========Run a query and see the output
sql_query='select * from table where col1="Dhaka"'
run_query_output(conn,sql_query)

#========Run query and get output as a dataframe
#make sure the query returns data
sql_query='select * from table'
df=query_to_df(conn,sql_query)

#=======Run query that does manupulation 
sql_query="update table set num=2 where name='ABC'"
run_query(sql,conn)

#======Lastly Close the connection
close_connection(conn)