import psycopg2

def insert_StoreStatus():
    try:
        conn = psycopg2.connect("dbname=loop user=postgres password=tanmay host=localhost port=5432")
        cursor = conn.cursor()
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\CSV_Files\store status.csv', 'r') as f:
            next(f) # skip the header row
            cursor.copy_from(f, 'store_status', sep=',')
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()

def insert_StoreTimezone():
    try:
        conn = psycopg2.connect("dbname=loop user=postgres password=tanmay host=localhost port=5432")
        cursor = conn.cursor()
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\CSV_Files\store_timezone.csv', 'r') as f:
            next(f) # skip the header row
            cursor.copy_from(f, 'store_timezone', sep=',')
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()

def insert_MenuHours():
    try:
        conn = psycopg2.connect("dbname=loop user=postgres password=tanmay host=localhost port=5432")
        cursor = conn.cursor()
        with open(r'C:\Users\Dell\Documents\JobApplicationProjects\loop\CSV_Files\Menu hours.csv', 'r') as f:
            next(f) # skip the header row
            cursor.copy_from(f, 'menu_hours', sep=',')
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()

# This File will run only once to save CSV data in PostgresSQL Server on Render.com      
if __name__=='__main__':
    insert_StoreStatus()
    # insert_MenuHours()
    # insert_StoreTimezone()
    pass
    