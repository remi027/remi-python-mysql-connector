import mysql.connector

# Print stuff, specially useful if you want to check tables columns orders
debug = False

def insert(cursor, table, args):
    """
    Currently it doesn't work with C ext
        If your mysql.connector is version 2.1.1 or newer:
        Enable "use_pure = True" during the login

    Errors are returned with HTML error response codes

    This function receives:
        The cursor from mysql.connector
        The table where you want to insert things
            The columns will be sorted by 'ORDER BY column_name ASC' -- (line 58)
        And the data you want to insert
            The data must be either:
                A tuple with the values for a single line insert
                    (foo, bar, baz)
                A list with tuples inside for bulk, multiline insert
                    [(foo, bar), (baz, qux)]
            The amount should be the same as the database columns

    Examples of usage:
    Single INSERT:
    # insert("table", ("value", ))
    Batch insert:
    # insert("table", [("aaa", ), ("bbb", ),])
    """
    ###DEBUG###
    if debug:
        print("Cursor Obejct: ", cursor)
        print("Table param: ", table)
        print("Data to be inserted: ", args)
    ###DEBUG###

    # Type check, ensures data integrity
    if not(isinstance(cursor, mysql.connector.cursor.MySQLCursor)):
        if(isinstance(cursor, mysql.connector.cursor_cext.CMySQLCursor)):
            print("Error:\n    MySQL cursor error.\n    Using mysql.connector C ext, enable 'use_pure = True' during login.")
            raise ValueError(405)
        else:
            print(f"Error:\n    MySQL cursor error.\n    Required <mysql.connector.cursor.MySQLCursor>, given {type(cursor)}.")
    if not(isinstance(table, str)):
        print("Error:\n    Table type error.\n    String required.")
        raise ValueError(405)
    if(isinstance(args, tuple)):
        is_bulk = False
    elif(isinstance(args, list)):
        is_bulk = True
    else:
        print("Error:\n    Insert data error.\n    Must be either tuple or list.")
        raise ValueError(405)

    # Injection protection, might use a bit more work
    forbidden_strings = [ ";", "=", "--", "DROP"]
    for i in forbidden_strings:
        if(i in str(table) or i in str(args)):
            print(f"INJECTION DETECTED IN TABLE.\nINSERT TERMINATED.Error string '{i}' on '{table}'")
            raise ValueError(405)
        if i in str(args):
            print(f"INJECTION DETECTED IN DATA.\nINSERT TERMINATED.\nError string '{i}' on '{args}'")
            raise ValueError(405)

    # Select column names
    cursor.execute("""SELECT column_name
        FROM information_schema.columns
        WHERE table_name=%s
        ORDER BY column_name ASC""", (table, ))
    table_return = cursor.fetchall()

    # Remove id columns from the query
    # You might want to remove or rework this based on how your db works
    for i in table_return:
        for ii in i:
            if "id" in ii:
                table_return.pop(table_return.index(i))
    ###DEBUG###
    if debug:
        print(table_return)
    ###DEBUG###

    # Batch insert
    if(is_bulk == True):
        print("Batch insert detected.")
        c = 0
        # Batch sanity check and string manipulation
        for i in args:
            if not(isinstance(args[c], tuple)):
                print(f"Error:\n Batch insert detected but {args[c]} not tuple.")
                raise ValueError(400)
            # Batch column and args sanity check
            if(len(table_return) != len(args[c])):
                print(f'INSERT ERROR\nRequired {len(table_return)} columns but {len(args[c])} given on {args[c]}.')
                raise ValueError(422)
            c += 1
        print(f"{c} args to batch insert.")
        # Transforming the columns and the amount of columns into strings
        columns = ', '.join([i[0] for i in table_return])
        amountcolumns = ', '.join(["%s" for i in range(len(table_return))])
        # Forming the insert string
        string = f"INSERT INTO {table} ({columns}) VALUES ({amountcolumns})"

        ###DEBUG###
        if debug:
            print("Final string: ", string)
        try:
            return cursor.executemany(string, args)
        except:
            raise ValueError("ERROR EXECUTEMANY BULK.")
        ###DEBUG###

        # Executing and returning the result
        return cursor.executemany(string, args)

    # Single input insert
    elif(is_bulk == False):
        # Single column and args sanity check
        if(len(table_return) != len(args)):
            print(f'INSERT ERROR\nRequired {len(table_return)} columns but {len(args)} given.')
            raise ValueError(422)
        # Transforming the columns and the amount of columns into strings
        columns = ', '.join([i[0] for i in table_return])
        amountcolumns = ', '.join(["%s" for i in range(len(table_return))])
        # Forming the insert string
        string = f"INSERT INTO {table} ({columns}) VALUES ({amountcolumns})"

        ###DEBUG###
        if debug:
            print("Final string: ", string)
        try:
            return cursor.executemany(string, args)
        except:
            raise ValueError("ERROR EXECUTE SINGLE.")
        ###DEBUG###

        # Executing and returning the result
        return cursor.execute(string, args)
