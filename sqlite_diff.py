#! Python 3.7
import sqlite3
from sqlite3 import Error

list_table_query = "select name from sqlite_master where type = 'table'"

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def get_common_tables(old_conn, new_conn):
    """ a comparison function which checks for tables with the same name
    :param 
        old_conn: the connection to the old db
        new_conn: the connection to the new db
    :return: A list of table names
    """

    old_conn.row_factory = lambda cursor, row: row[0]
    new_conn.row_factory = lambda cursor, row: row[0]

    old_cursor = old_conn.cursor()
    new_cursor = new_conn.cursor()

    old_tables = old_cursor.execute(list_table_query).fetchall()
    new_tables = new_cursor.execute(list_table_query).fetchall()

    # no need for any fancy optimized algorithms since this is always O(n). list has no repeated items
    return [value for value in new_tables if value in old_tables] 


def get_table_structure_diff(old_conn, new_conn):
    """ compares tables which exist in both DBs and checks to see 
        if there are any differences between the two.
    :param 
        old_conn: the connection to the old db
        new_conn: the connection to the new db
    """
    
    old_cursor = old_conn.cursor()
    new_cursor = new_conn.cursor()
    common_tables = get_common_tables(old_conn, new_conn)

    pragma_table_info_query = "PRAGMA table_info(`{}`)"
    select_all_rows_query = "SELECT `{}` FROM `{}`"
    except_rows_query = "SELECT `{}` FROM `{}` WHERE `{}` NOT IN ({})"

    for table in common_tables:
        old_schema = old_cursor.execute(pragma_table_info_query.format(table)).fetchall()
        new_schema = new_cursor.execute(pragma_table_info_query.format(table)).fetchall()
        print("### %s ###" % table)

        # selecting first column of each table?
        # get difference in rows
        if (old_schema == new_schema):
            pk = new_schema[0][1]
            
            old_rows = old_cursor.execute(select_all_rows_query.format(pk, table)).fetchall()
            new_rows = new_cursor.execute(select_all_rows_query.format(pk, table)).fetchall()
            
            old_rows_ids = [row[0] for row in old_rows]
            new_rows_ids = [row[0] for row in new_rows]

            if (old_rows != new_rows):
                ''' 
                    1. old rows that do not exist in new table should be removed (NOT IN new_rows)
                    2. new rows that do not exist in old table should be added (NOT IN old_rows)
                    3. find rows with difference in data (TBD)
                    4. generate SQL statement for these
                '''

                delete_rows = old_cursor.execute(except_rows_query.format(pk, table, pk, str(new_rows_ids)[1:-1])).fetchall()
                add_rows = new_cursor.execute(except_rows_query.format(pk, table, pk, str(old_rows_ids)[1:-1])).fetchall()

                print("Rows to be deleted " + str(delete_rows))
                print("Rows to be added " + str(add_rows))


if __name__ == '__main__':
    old = input("Please enter the name of the older database file: ")
    old_conn = create_connection(old)

    while(old_conn is None):
        old = input("Invalid database file, please enter again: ")
        old_conn = create_connection(old)

    new = input("Please enter the name of the newer database file: ")
    new_conn = create_connection(new)

    while(old_conn is None):
        old = input("Invalid database file, please enter again: ")
        old_conn = create_connection(old)

    get_table_structure_diff(old_conn, new_conn)
