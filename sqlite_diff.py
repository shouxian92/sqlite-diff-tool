#! Python 3.7
import sqlite3
from sqlite3 import Error

DEBUG = True
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

def get_primary_key(conn, table, columns):
    """ attempts to reverse lookup the primary key by querying the table using the first column
        and iteratively adding the columns that comes after it until the query returns a
        unique row in the table.
    :param
        conn: an SQLite connection object
        table: a string denoting the table name to query
        columns: a list containing column names of the table
    :return: the list of columns which makes up the primary key
    """

    select_row_query = "SELECT * FROM `{}`".format(table)
    count_row_query = "SELECT COUNT(*) FROM `{}` WHERE `{}`"
    primary_key = []

    row = conn.execute(select_row_query).fetchone()

    if row is not None:
        lastCount = 0
        for i, column in enumerate(columns):
            if i == 0:
                count_row_query = count_row_query.format(table, column)
            else:
                count_row_query += " AND `{}`".format(column)
            
            if row[i] is None:
                count_row_query += "=NULL"
            elif type(row[i]) in (int, float, bool):
                count_row_query += "={}".format(row[i])
            else:
                count_row_query += "='{}'".format(row[i])

            primary_key.append(column)
            count = conn.execute(count_row_query).fetchone()
            lastCount = count[0]

            if count[0] == 1:
                return primary_key
    
    # if no primary key was found then the primary key is made up of all columns
    return columns
    

def get_table_structure_diff(old_conn, new_conn, old_db_filename, new_db_filename):
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
    select_column_rows_query = "SELECT {} FROM `{}`"
    select_all_rows_query = "SELECT * FROM `{}`"

    #attach_query = "ATTACH DATABASE `{}` AS `{}`".format(new_db_filename, "new")
    #old_cursor.execute(attach_query)

    equal = 0
    notequal = 1
    for table in common_tables:
        old_schema = old_cursor.execute(pragma_table_info_query.format(table)).fetchall()
        new_schema = new_cursor.execute(pragma_table_info_query.format(table)).fetchall()
        print("### %s ###" % table)

        # get difference in rows
        if (old_schema == new_schema):
            columns = [col[1] for col in old_schema]
            pk = get_primary_key(old_cursor, table, columns)
            data_cols = set(columns) - set(pk)
            #print("primary: {} ({}), others: {} ({})".format(pk, len(pk), others, len(data)))

            pk = ", ".join('`{0}`'.format(k) for k in pk)

            # this will generate the statement to select "primary key"
            select_by_pk = select_column_rows_query.format(pk, table) 

            if len(data_cols) == 0:
                # get everything if the primary key are all the columns
                select_rows_stmt = select_all_rows_query.format(table)
                old_row_data = old_cursor.execute(select_rows_stmt).fetchall()
                new_row_data = new_cursor.execute(select_rows_stmt).fetchall()
            else:
                data_cols = ", ".join('`{}`'.format(k) for k in data_cols)
                select_rows_stmt = select_column_rows_query.format(data_cols, table)
                old_row_data = old_cursor.execute(select_rows_stmt).fetchall()
                new_row_data = new_cursor.execute(select_rows_stmt).fetchall()

            old_rows_ids = old_cursor.execute(select_by_pk).fetchall()
            new_rows_ids = new_cursor.execute(select_by_pk).fetchall()
            
            if (old_rows_ids != new_rows_ids):
                ''' 
                    1. old rows that do not exist in new table should be removed (NOT IN new_rows)
                    2. new rows that do not exist in old table should be added (NOT IN old_rows)
                    3. find rows with difference in data (TBD)
                    4. generate SQL statement for these
                '''
                notequal += 1


                # hash row ids to make comparison
                old_row_ids_hashed = [hash(rowid) for rowid in old_rows_ids]
                new_row_ids_hashed = [hash(rowid) for rowid in new_rows_ids]
                
                # hash data rows as well (why not)
                old_row_data_hashed = [hash(old_row) for old_row in old_row_data]
                new_row_data_hashed = [hash(new_row) for new_row in new_row_data]
                
                old_hashmap = dict(zip(old_row_ids_hashed, old_row_data_hashed))
                new_hashmap = dict(zip(new_row_ids_hashed, new_row_data_hashed))

                not_inside_count = 0
                inside_count = 0
                new_row_count = 0
                data_changed = 0
                for old_hashed_pk in old_row_ids_hashed:
                    # Attempts to get the row information from the hashed primary key
                    # returns false if not present in the dictionary
                    in_new_table = new_hashmap.get(old_hashed_pk, False)

                    if not in_new_table:
                        # generate delete statement
                        not_inside_count += 1
                    else:
                        if in_new_table != old_hashmap[old_hashed_pk]:
                            print("Comparing hash in new table ({}) to old ({})".format(in_new_table, old_hashmap[old_hashed_pk]))
                            # TODO: go look for the data which belongs to the new data and then do an UPDATE SET statement for starters
                            data_changed += 1

                        inside_count += 1

                for new_hash_pk in new_row_ids_hashed:
                    in_old_table = old_hashmap.get(new_hash_pk, False)

                    if not in_old_table:
                        # generate insert statement
                        new_row_count += 1
                
                if DEBUG:
                    print("{} rows identical to new table".format(inside_count))
                    print("{} rows not in new table".format(not_inside_count))
                    print("{} new rows from new table".format(new_row_count))
                    print("{} rows were changed in new table".format(data_changed))


            else:
                equal += 1
                print("Tables are identical")
    
    if DEBUG:
        print("Total - not equal: {}. equal: {}".format(notequal, equal))

if __name__ == '__main__':
    old = "heroesContentsOrig.db3" #input("Please enter the name of the older database file: ")
    old_conn = create_connection(old)

    while(old_conn is None):
        old = input("Invalid database file, please enter again: ")
        old_conn = create_connection(old)

    new = "heroesContentsFashion.db3" # input("Please enter the name of the newer database file: ")
    new_conn = create_connection(new)

    while(old_conn is None):
        old = input("Invalid database file, please enter again: ")
        old_conn = create_connection(old)

    get_table_structure_diff(old_conn, new_conn, old, new)
