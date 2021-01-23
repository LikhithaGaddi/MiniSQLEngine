import csv
import sys
import re  # regular expression
import sqlparse
from sqlparse.tokens import Keyword, DML
from sqlparse.sql import IdentifierList, Identifier, Where

# reading metadata.txt file
f = open("data/metadata.txt", "r")
lines = f.readlines()

# storing tables metadata
start = 0
tables = []
table = []
for line in lines:
    if (line[:-1] == "<begin_table>"):
        start = 1
        pass
    elif (line[:-1] == "<end_table>"):
        start = 0
        tables.append(table)
        table = []
    elif (start == 1):
        table.append(line[:-1])

if (len(table) != 0):
    table.pop(-1)
    tables.append(table)

# mapping tablenames with respect to their rows
tables = {x[0]: x[1:len(x)] for x in tables}

def verify_column_name(input,columns_to_tables):
    agg_functions = ["max","min", "sum", "count", "avg"]
    for col in agg_functions:
        if input.startswith(col):
            return col+"("+columns_to_tables[input.split('(')[1].split(')')[0]]+"."+input+")"
            # return input.split('(')[1].split(')')[0]
    return columns_to_tables[input]+"."+input

# printing output
def print_output(columns_data, table_names):
    # column names to table names mapping
    columns_to_tables = {}
    column = ""
    for table in table_names:
        for col in tables[table]:
            columns_to_tables[col] = table
    for col in columns_data.keys():
        column = col
        if col == "count(*)":
            print(col.lower(), end='')
        else:
            print(verify_column_name(col,columns_to_tables).lower(),end='')
        if list(columns_data.keys())[-1] != col:
            print(",", end='')
    print("")
    for i in range(0, len(columns_data[column])):
        for col in columns_data.keys():
            print(columns_data[col][i], end='')
            if list(columns_data.keys())[-1] != col:
                print(",", end='')
            else:
                print("")
    print("\n")

# extracting data from csv file
def get_data_from_csv(query_tables):
    # creating a dictionary for every columns in all tables
    columns_data = {}
    for table in tables:
        if table in query_tables:
            for col in tables[table]:
                columns_data[col] = []

    # reading data from csv files
    for table in tables:
        if table in query_tables:
            number_of_columns = len(tables[table])
            filename = "data/" + table + ".csv"
            with open(filename) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    for i in range(number_of_columns):
                        columns_data[tables[table][i]].append(int(row[i].replace('"','')))

    return columns_data

# Extracting table names from the query
def get_table_names(query):
    from_seen = False
    tables = ""
    query = sqlparse.format(query, reIndent=True, keyword_case='upper')
    parsed = sqlparse.parse(query)
    for stmt in parsed:
        tokens = [t for t in sqlparse.sql.TokenList(
            stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        for item in tokens:
            if from_seen:
                if item.ttype is Keyword or isinstance(item, Where):
                    break
                else:
                    tables = str(item)
            elif item.ttype is Keyword and item.value.upper() == 'FROM':
                from_seen = True
    return [x.strip() for x in tables.split(',')]

# cross-join of two tables
def cross_join_two(table_one, table_two):
    col_data = {}
    cloumns = []
    col1 = ""
    col2 = ""
    for key in table_one.keys():
        col1 = key
        cloumns.append(key)

    for key in table_two.keys():
        col2 = key
        cloumns.append(key)

    for col in cloumns:
        col_data[col] = []

    len1 = len(table_one[col1])
    len2 = len(table_two[col2])
    for i in range(0, len1):
        for j in range(0, len2):
            for col in cloumns:
                if col in table_one.keys():
                    col_data[col].append(table_one[col][i])
                elif col in table_two.keys():
                    col_data[col].append(table_two[col][j])
    return col_data

# cross-join of multiple tables
def cross_join_tables(columns_data, table_names):
    n_tables = len(table_names)
    join_data = {}
    for col in tables[table_names[0]]:
        join_data[col] = columns_data[col]
    for i in range(1, n_tables):
        data = {}
        for col in tables[table_names[i]]:
            data[col] = columns_data[col]
        join_data = cross_join_two(join_data, data)
    return join_data

# evaluate where expression
def eval_expression(keys, val,columns_data,index):
    col = ""
    for x in val:
        if x == '=' or x == '>' or x == '<':
            break
        else:
            col += x
    if col not in keys:
        print("Error: column name not in the tables given")
        exit(0)
    col2 = ""
    for x in val[::-1]:
        if x == '=' or x == '>' or x == '<':
            break
        else:
            col2 += x
    col2 = col2[::-1]
    operator = ""
    if str(val[len(col):len(val) - len(col2)]) == "=":
        operator = "=="
    else:
        operator = val[len(col):len(val) - len(col2)]
    if col2.isdigit():
        return str(columns_data[col][index])+""+operator+""+str(col2)
    if col2 not in keys:
        print("Error: column name not in the tables given")
        exit(0)
    return str(columns_data[col][index])+""+operator+""+str(columns_data[col2][index])

# execute where clause
def execute_where(columns_data, query):
    query = sqlparse.format(query, reIndent=True, keyword_case='upper')
    parsed = sqlparse.parse(query)
    for stmt in parsed:
        tokens = [t for t in sqlparse.sql.TokenList(
            stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        where_stmt = ""
        for token in tokens:
            if token.value.startswith("WHERE"):
                where_stmt = str(token)
                break
        index = 0
        for ch in where_stmt:
            if (ch == " "):
                break
            else:
                index += 1
        where_stmt = where_stmt[index + 1:]
        where_stmt = where_stmt.strip()
        values = re.split(' +', where_stmt)
        index = 0
        operator = ""
        found_op = False
        before = ""
        after = ""
        for value in values:
            if value == "AND" or value == "OR":
                operator = value
                found_op = True
            elif found_op == False:
                before += value.strip()
            else:
                after += value.strip()
        if (operator == ""):
            cols = []
            for key in columns_data.keys():
                cols.append(key)
            where_data = {}
            for key in cols:
                where_data[key] = []
            for i in range(0, len(columns_data[cols[0]])):
                expr = eval_expression(cols, before, columns_data, i)
                if eval(expr):
                    for key in cols:
                        where_data[key].append(columns_data[key][i])
            return where_data
        else:
            if (operator == "AND" or operator == "OR"):
                cols = []
                for key in columns_data.keys():
                    cols.append(key)
                data = {}
                for key in cols:
                    data[key] = []
                for i in range(0, len(columns_data[cols[0]])):
                    expr = eval_expression(cols, before, columns_data, i)
                    expr2 = eval_expression(cols, after, columns_data, i)
                    expr = expr+" "+operator.lower()+" " + expr2
                    if eval(expr):
                        for key in cols:
                            data[key].append(columns_data[key][i])
                return data
            else:
                print("Error: Invalid operator in where")
                exit(0)

# get column names
def get_column_names(columns):
    return [x.strip() for x in columns.split(',')]

# execute group by
def execute_group_by(columns_data, select, group_by, table_names, has_distinct, distinct, has_order_by, order_by):
    column = group_by.strip()
    if column not in columns_data.keys():
        print("Error: Invalid column name in group by stmt")
        exit(0)
    if has_distinct:
        select = get_column_names(distinct)
    else:
        select = get_column_names(select)
    has_aggregate_function = False
    agg_function = {}
    has_group_by_col = False
    for col in select:
        if col != column:
            if col.lower().startswith("max"):
                has_aggregate_function = True
                if "max" not in agg_function.keys():
                    agg_function["max"] = [col]
                else:
                    agg_function["max"].append(col)
            elif col.lower().startswith("min"):
                has_aggregate_function = True
                if "min" not in agg_function.keys():
                    agg_function["min"] = [col]
                else:
                    agg_function["min"].append(col)
            elif col.lower().startswith("sum"):
                has_aggregate_function = True
                if "sum" not in agg_function.keys():
                    agg_function["sum"] = [col]
                else:
                    agg_function["sum"].append(col)
            elif col.lower().startswith("avg"):
                has_aggregate_function = True
                if "avg" not in agg_function.keys():
                    agg_function["avg"] = [col]
                else:
                    agg_function["avg"].append(col)
            elif col.lower().startswith("count"):
                has_aggregate_function = True
                if "count" not in agg_function.keys():
                    agg_function["count"] = [col]
                else:
                    agg_function["count"].append(col)
            else:
                print("Error: Invalid column name in select stmt")
                exit(0)
        else:
            has_group_by_col = True
    if (len(select) > 1 and has_aggregate_function == False) or not has_group_by_col:
        print("Error: Invalid column names in select stmt")
        exit(0)
    group_data = {}
    for i in range(0, len(columns_data[column])):
        if columns_data[column][i] in group_data.keys():
            group_data[columns_data[column][i]].append(i)
        else:
            group_data[columns_data[column][i]] = [i]

    final_group_by_data = {}
    if has_group_by_col:
        final_group_by_data[column] = list(group_data.keys())
    agg_functions = {"sum": sum, "max": max, "min": min}
    for agg in agg_function.keys():
        for i in range(0,len(agg_function[agg])):
            final_group_by_data[agg_function[agg][i]] = []
            col_name = agg_function[agg][i].split("(")[1].split(")")[0]
            if col_name == column or (col_name not in columns_data.keys()):
                print("Error: Invalid column name in group by")
                exit(0)
            for x in group_data.keys():
                l = []
                for index in group_data[x]:
                    l.append(columns_data[col_name][index])
                if agg in agg_functions:
                    final_group_by_data[agg_function[agg][i]].append(
                        agg_functions[agg](l))
                elif agg == "avg":
                    final_group_by_data[agg_function[agg][i]].append(
                        agg_functions["sum"](l) / len(l))
                elif agg == "count":
                    final_group_by_data[agg_function[agg][i]].append(len(l))
                else:
                    print("Error: Invalid aggregate function")
                    exit(0)
    if has_distinct and has_group_by_col == False:
        data = set()
        column_name = list(agg_function.keys())
        for i in range(0, len(final_group_by_data[list(agg_function.keys)[0]])):
            x = tuple()
            for key in column_name:
                y = (final_group_by_data[key][i],)
                x = (x + y)
            data.add(x)
        return_data = {}
        for col in column_name:
            return_data[col] = []
        for val in data:
            for i in range(0, len(val)):
                return_data[column_name[i]].append(val[i])
    else:
        return_data = final_group_by_data
    # print("has order by: " + str(has_order_by))
    if (has_order_by and not has_group_by_col) or (has_order_by and order_by.split(" ")[0] != column):
        print("Error: Invalid order by column name")
        exit(0)
    elif has_order_by:
        order_by = order_by.split(" ")
        column_name = order_by[0].strip()
        if len(order_by) == 1:
            order = ""
        else:
            order = order_by[1].strip()
        if column_name == "":
            print("Error: order by condition is not mentioned properly")
            exit(0)
        if column_name not in columns_data.keys():
            print("Error: Column name in order by is not present in table")
            exit(0)
        column_to_index = {}
        for i in range(0, len(return_data[column_name])):
            column_to_index[return_data[column_name][i]] = i
        return_datas = {}
        colums = []
        for col in return_data.keys():
            return_datas[col] = []
            colums.append(col)
        if order == "DESC":
            for key, value in reversed(sorted(column_to_index.items())):
                for col in colums:
                    return_datas[col].append(return_data[col][value])
        elif order == "ASC" or order == "":
            for key, value in sorted(column_to_index.items()):
                for col in colums:
                    return_datas[col].append(return_data[col][value])
        else:
            print("Error: order mentioned is wrong")
            exit(0)
        return_data = return_datas
    print_output(return_data, table_names)
    exit(0)

# execute select 
def execute_select(columns_data, select):
    if select == "":
        print("Error: select columns are not sepcified")
        exit(0)
    if select == "*":
        return columns_data
    columns = get_column_names(select)
    has_aggregate_function = False
    has_normal_col = False
    agg_function = {}
    for col in columns:
        if col.lower().startswith("max"):
            has_aggregate_function = True
            if "max" not in agg_function.keys():
                agg_function["max"] = [col]
            else:
                agg_function["max"].append(col)
        elif col.lower().startswith("min"):
            has_aggregate_function = True
            if "min" not in agg_function.keys():
                agg_function["min"] = [col]
            else:
                agg_function["min"].append(col)
        elif col.lower().startswith("sum"):
            has_aggregate_function = True
            if "sum" not in agg_function.keys():
                agg_function["sum"] = [col]
            else:
                agg_function["sum"].append(col)
        elif col.lower().startswith("avg"):
            has_aggregate_function = True
            if "avg" not in agg_function.keys():
                agg_function["avg"] = [col]
            else:
                agg_function["avg"].append(col)
        elif col.lower().startswith("count"):
            has_aggregate_function = True
            if "count" not in agg_function.keys():
                agg_function["count"] = [col]
            else:
                agg_function["count"].append(col)
        elif col in columns_data.keys() and not has_aggregate_function:
            has_normal_col = True
        else:
            print("Error: Invalid column name in select stmt")
            exit(0)
    if has_aggregate_function and has_normal_col:
        print("Error: Invalid column names in select")
        exit(0)
    if (has_aggregate_function):
        return_data = {}
        agg_functions = {"sum": sum, "max": max, "min": min}
        for agg in agg_function.keys():
            for i in range(0, len(agg_function[agg])):
                return_data[agg_function[agg][i]] = []
                col_name = agg_function[agg][i].split("(")[1].split(")")[0]
                if (col_name not in columns_data.keys()) and (agg!="count" and col_name!="*"):
                    print("Error: Invalid column name in group by")
                    exit(0)
                # for col in columns_data.keys():
                l = []
                length = len(columns_data[list(columns_data.keys())[0]])
                if col_name == "*":
                    return_data[agg_function[agg][i]].append(length)
                else:
                    for index in range(0,length):
                        l.append(columns_data[col_name][index])
                    if agg in agg_functions:
                        return_data[agg_function[agg][i]].append(
                            agg_functions[agg](l))
                    elif agg == "avg":
                        return_data[agg_function[agg][i]].append(
                            agg_functions["sum"](l) / len(l))
                    elif agg == "count":
                        return_data[agg_function[agg][i]].append(len(l))
                    else:
                        print("Error: Invalid aggregate function")
                        exit(0)
        return return_data
    to_del = [key for key in columns_data if key not in columns]
    for key in to_del:
        del columns_data[key]
    return columns_data

# execute distinct
def execute_distinct(columns_data, column_name):
    columns_data = execute_select(columns_data, column_name)
    if(column_name != "*"):
        column_name = get_column_names(column_name)
    else:
        column_name = list(columns_data.keys())
    data=set()
    for i in range(0, len(columns_data[column_name[0]])):
        x=tuple()
        for key in column_name:
            y=(columns_data[key][i],)
            x=(x + y)
        data.add(x)
    return_data={}
    for col in column_name:
        return_data[col]=[]
    for val in data:
        for i in range(0, len(val)):
            return_data[column_name[i]].append(val[i])
    return return_data

# execute order by
def execute_order_by(columns_data, order_by):
    order_by = order_by.split(" ")
    if (len(order_by) > 2):
        print("Error: Invalid order by")
        exit(0)
    column_name = order_by[0].strip()
    if len(order_by) == 1:
        order = ""
    else:
        order = order_by[1].strip()
    if column_name == "":
        print("Error: order by condition is not mentioned properly")
        exit(0)
    if column_name not in columns_data.keys():
        print("Error: Column name in order by is not present in table")
        exit(0)    
    column_to_index = {}
    for i in range(0, len(columns_data[column_name])):
        column_to_index[columns_data[column_name][i]] = i
    return_data = {}
    colums = []
    for col in columns_data.keys():
        return_data[col] = []
        colums.append(col)
    if order == "DESC":
        for key, value in reversed(sorted(column_to_index.items())):
            for col in colums:
                return_data[col].append(columns_data[col][value])
    elif order == "ASC" or order == "":
        for key, value in sorted(column_to_index.items()):
            for col in colums:
                return_data[col].append(columns_data[col][value])
    else:
        print("Error: order mentioned is wrong")
        exit(0)
    return return_data

# executing the query
def execute_query(query):
    table_names = get_table_names(query)
    for table in table_names:
        if table not in tables:
            print("Error: " + str(table) + " doesn't exists")
    columns_data = get_data_from_csv(table_names)
    query = sqlparse.format(query, reIndent=True, keyword_case='upper')
    parsed = sqlparse.parse(query)
    group_by = ""
    order_by = ""
    distinct = ""
    select = ""
    has_select = False
    has_where = False
    has_group_by = False
    has_order_by = False
    has_distinct = False
    has_from = False
    for stmt in parsed:
        tokens = [t for t in sqlparse.sql.TokenList(
            stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        index = 0
        for token in tokens:
            if token.value.startswith("WHERE"):
                has_where = True
            elif token.value.startswith("GROUP BY") and not (tokens[index+1].ttype == Identifier or tokens[index+1].ttype == IdentifierList):
                has_group_by = True
                if (index + 1 >= len(tokens)):
                    print("Error: Invalid Query. Missing column name for group by")
                    exit(0)
                group_by = str(tokens[index+1])
            elif token.value.startswith("ORDER BY") and not (tokens[index+1].ttype == Identifier or tokens[index+1].ttype == IdentifierList):
                has_order_by = True
                if (index + 1 >= len(tokens)):
                    print("Error: Invalid Query. Missing column name for order by")
                    exit(0)
                order_by = str(tokens[index+1])
            elif token.value.startswith("DISTINCT") and not (tokens[index+1].ttype == Identifier or tokens[index+1].ttype == IdentifierList):
                has_distinct = True
                if (index + 1 >= len(tokens)):
                    print("Error: Invalid Query. Missing column name after DISTINCT")
                    exit(0)
                distinct = str(tokens[index + 1])
            elif token.value.startswith("SELECT"):
                has_select = True
                if (index + 1 >= len(tokens) and not (tokens[index+1].ttype == Identifier or tokens[index+1].ttype == IdentifierList or tokens[index+1].value.startswith("DISTINCT"))):
                    print("Error: Invalid Query. Missing column name for select stmt")
                    exit(0)
                select = str(tokens[index + 1])
            elif token.value.startswith("FROM") and not (tokens[index+1].ttype == Identifier or tokens[index+1].ttype == IdentifierList):
                has_from = True
                if (index + 1 >= len(tokens)):
                    print("Error: Invalid Query. Missing table name")
                    exit(0)
            index+=1

    if not has_select or not has_from:
        print("Error: there is no select stmt")
        exit(0)

    if(len(table_names) > 1):
        columns_data = cross_join_tables(columns_data, table_names)
    
    if has_where:
        columns_data = execute_where(columns_data, query)

    if has_group_by:
        execute_group_by(columns_data, select, group_by, table_names,has_distinct,distinct,has_order_by, order_by)

    if has_select and not has_distinct:
        columns_data = execute_select(columns_data,select)

    if has_distinct:
        columns_data = execute_distinct(columns_data,distinct)

    if has_order_by:
        columns_data = execute_order_by(columns_data,order_by)
  
    print_output(columns_data, table_names)

# process the query
def process_query(query):
    # validate the query first    
    if query.isspace() or len(query) == 0:
        print("Error: Query is empty")
        exit(0)
    execute_query(query)

# main function
if __name__ == '__main__':
    # reading the query from command line
    query = ""
    for i in range(1, len(sys.argv)):
        query += str(sys.argv[i]) +" "
    query = query[:-1]
    if (query[-1] != ";"):
        print("Error: Semicolon is missing")
        exit(0)
    process_query(query[:-1])
