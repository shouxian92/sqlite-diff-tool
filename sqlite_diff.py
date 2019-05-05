#! Python 3.7
import sqlite3
import os
from datetime import datetime
from sqlite3 import Error

DEBUG = False

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

    list_table_query = "select name from sqlite_master where type = 'table'"

    old_conn.row_factory = lambda cursor, row: row[0]
    new_conn.row_factory = lambda cursor, row: row[0]

    old_cursor = old_conn.cursor()
    new_cursor = new_conn.cursor()

    old_tables = old_cursor.execute(list_table_query).fetchall()
    new_tables = new_cursor.execute(list_table_query).fetchall()

    # no need for any fancy optimized algorithms since this is always O(n). list has no repeated items
    return [value for value in new_tables if value in old_tables] 

def format_sqlite_value(in_value):
    """ will add quotes around it if its a string or convert the string to NULL if it is a python None value
    :param
        in_value: an object that is to be returned with or without quotes. NULL if None
    """
    if type(in_value) in (int, float, bool):
        return str(in_value)
    elif in_value is None:
        return "NULL"
    else:
        # escape strings with single-quotes
        in_value = in_value.replace("'", "''")
        return "'{}'".format(in_value)

def append_eql_condition(in_value, update = False):
    """ appends an equal condition to a string but the function will
        add quotes around it if its a string
    :param
        in_value: an object that is to be converted to the equals clause
    """
    if in_value is None and not update:
        return " is " + format_sqlite_value(in_value)
    else:
        return "=" + format_sqlite_value(in_value)

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
        for i, column in enumerate(columns):
            if i == 0:
                count_row_query = count_row_query.format(table, column)
            else:
                count_row_query += " AND `{}`".format(column)
            
            count_row_query += append_eql_condition(row[i])

            primary_key.append(column)
            count = conn.execute(count_row_query).fetchone()

            if count[0] == 1:
                return primary_key
    
    # if no primary key was found then the primary key is made up of all columns
    return columns

def equal_stmt_list_generator(columns, data, update = False):
    statement_list = []
    for col_idx, col_value in enumerate(data):
        col_name = columns[col_idx]
        statement_list.append('`{}`'.format(col_name) + append_eql_condition(col_value, update))
    
    return statement_list

def generate_insert_query(table, pk, data, column = ''):
    """ generates a UPDATE query with WHERE clauses given a table name and columns
        and sets it to the given data
    :params
        table: the name of the table
        pk: the primary key data for the table
        data: the data that does not belong to the primary key
        column: column names to generate for the insert statement. this is optional since some times we are 100% sure of the column order
    :return: an INSERT statement string
    """

    insert_query_template = "INSERT INTO `{}` {} VALUES ({});"
    
    if data is None:
        data_list = [format_sqlite_value(key) for key in pk]
    else:
        data_list = [format_sqlite_value(key) for key in pk] + [format_sqlite_value(d) for d in data]

    return insert_query_template.format(table, column, ', '.join(data_list))


def generate_update_query(table, where_cols, where_data, update_cols, update_data):
    """ generates a UPDATE query with WHERE clauses given a table name and columns
        and sets it to the given data
    :params
        table: the name of the table
        where_cols: the column names to use for adding WHERE clause
        where_data: the data matching the where columns
        update_cols: the columns names to use for setting the SET values
        update_data: the data used to set the values of the update SET
    :return: a UPDATE statement with WHERE clauses
    """

    update_query_template = "UPDATE `{}` SET {} WHERE {};"
    update_set = equal_stmt_list_generator(update_cols, update_data, True)
    where_clauses = equal_stmt_list_generator(where_cols, where_data)

    return update_query_template.format(table, ', '.join(update_set), ' AND '.join(where_clauses))
        
def generate_del_query(table, where_cols, row_data):
    """ generates a DELETE query with WHERE clauses given a table name and columns
    :params
        table: the name of the table
        where_cols: the column names to use for adding WHERE clause
        row_data: the data matching the where columns
    :return: a DELETE statement with WHERE clauses
    """

    if len(where_cols) != len(row_data):
        raise Exception("columns and row data are of different lengths")
    
    delete_query_template = "DELETE FROM `{}` WHERE {};"
    where_clauses = equal_stmt_list_generator(where_cols, row_data)
    
    return delete_query_template.format(table, ' AND '.join(where_clauses))

def get_table_data_diff(old_conn, new_conn, old_db_filename, new_db_filename):
    """ compares tables which exist in both DBs and checks to see 
        if there are any differences in rows between the two.
    :param 
        old_conn: the connection to the old db
        new_conn: the connection to the new db
    """
    diff_statements = []
    
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
        if DEBUG:
            print("### %s ###" % table)

        # get difference in rows
        if (old_schema == new_schema):
            columns = [col[1] for col in old_schema]
            orig_pk = get_primary_key(old_cursor, table, columns)
            data_cols_array = list(set(columns) - set(orig_pk))
            all_cols_are_pk = len(data_cols_array) == 0

            new_insert_order = orig_pk.copy()
            new_insert_order.extend(data_cols_array)
            new_insert_order = "(" + ", ".join("`{}`".format(c) for c in new_insert_order) + ")"

            if DEBUG:
                print("primary: {} ({}), others: {} ({})".format(orig_pk, len(orig_pk), data_cols_array, len(data_cols_array)))

            pk = ", ".join('`{0}`'.format(k) for k in orig_pk)

            # this will generate the statement to select "primary key"
            select_by_pk = select_column_rows_query.format(pk, table) 

            if all_cols_are_pk:
                # get everything if the primary key are all the columns
                select_rows_stmt = select_all_rows_query.format(table)
                old_row_data = old_cursor.execute(select_rows_stmt).fetchall()
                new_row_data = new_cursor.execute(select_rows_stmt).fetchall()
            else:
                data_cols = ", ".join('`{}`'.format(k) for k in data_cols_array)
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
                
                new_hashmap_pk_unhashed = dict(zip(new_row_ids_hashed, new_rows_ids))
                new_hashmap_data_unhashed = dict(zip(new_row_ids_hashed, new_row_data))

                not_inside_count, inside_count, new_row_count, data_changed = (0, 0, 0, 0)
                for index, old_hashed_pk in enumerate(old_row_ids_hashed):
                    # Attempts to get the row information from the hashed primary key
                    # returns false if not present in the dictionary
                    in_new_table = new_hashmap.get(old_hashed_pk, False)

                    where_cols = (data_cols_array, orig_pk) [all_cols_are_pk] #if all columns are pk, then data column is empty, switch to orig_pk
                    old_row = old_row_data[index]

                    if not in_new_table:
                        # generate delete statement
                        delete_where_string = generate_del_query(table, where_cols, old_row)  + '\n'

                        if DEBUG:
                            print(delete_where_string)

                        diff_statements.append(delete_where_string)
                        not_inside_count += 1
                    else:
                        if in_new_table != old_hashmap[old_hashed_pk]:

                            if DEBUG:
                                print("Comparing hash in new table ({}) to old ({})".format(in_new_table, old_hashmap[old_hashed_pk]))
                            # go look for the data which belongs to the new data and then do an UPDATE SET statement for starters
                            #update_string = generate_update_query(table, where_cols, old_row, where_cols, new_hashmap_data_unhashed[old_hashed_pk]) + '\n'
                            #diff_statements.append(update_string)
                            data_changed += 1

                        inside_count += 1

                for new_hash_pk in new_row_ids_hashed:
                    in_old_table = old_hashmap.get(new_hash_pk, False)

                    if not in_old_table:
                        # generate insert statement
                        new_pk = new_hashmap_pk_unhashed[new_hash_pk]
                        new_data = (new_hashmap_data_unhashed[new_hash_pk], None) [all_cols_are_pk] # pass nothing so we don't generate double insert values

                        diff_statements.append(generate_insert_query(table, new_pk, new_data, new_insert_order) + '\n')
                        new_row_count += 1
                
                if DEBUG:
                    print("{} rows identical to new table".format(inside_count))
                    print("{} rows not in new table".format(not_inside_count))
                    print("{} new rows from new table".format(new_row_count))
                    print("{} rows were changed in new table".format(data_changed))


            else:
                equal += 1
                if DEBUG:
                    print("Tables are identical")
    
    if DEBUG:
        print("Total - not equal: {}. equal: {}".format(notequal, equal))

    return diff_statements

def write_to_file(old_name, new_name, diff_statements, ext = "sql"):
    old_noext = os.path.splitext(old_name)[0]
    new_noext = os.path.splitext(new_name)[0]

    now = datetime.now()
    timestamp = datetime.timestamp(now)

    filename = "{}-{}-{}-diff.{}".format(old_noext, new_noext, timestamp, ext)
    f = open(filename, "w+", encoding="utf-8")
    f.writelines(diff_statements)
    f.close()

    return filename

def remove_dupes(seq):
    # f7 in https://www.peterbe.com/plog/uniqifiers-benchmark
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
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

    print('Both files are found and valid SQLite files.. making comparisons..')
    diff_statements = get_table_data_diff(old_conn, new_conn, old, new)
    
    diff_statements = remove_dupes(diff_statements)

    print('Comparison complete.')
    if len(diff_statements) > 0:
        filename = write_to_file(old, new, diff_statements)
        print('Diff file generated - {}'.format(filename))
    else:
        print('No difference found for the two files.')
