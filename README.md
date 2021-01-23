# Mini SQL Engine

MiniSQLEngine can parse the SQl queries and execute them taking input from .csv files. Developed using Python.

### Dataset:

- If a file is : File1.csv, the table name would be File1
- All the elements in files would be only integers
- A file named: metadata.txt will be used which has the follwing format

                <begin_table>
                <table_name>
                <attribute1>
                ......
                <attributeN>
                <end_table>

- Column names are unquie among all the tables. So column names are not preceded by table names in SQL queries


### Working

* Project columns from one or more tables:
    ```sh
    Select * from table_name;
    Select col1,col2 from table_name;
    ```
* Aggregate functions: Simple aggregate functions on a single column. Sum,average,max,min and count. they will be very trivial gien that the data is only integers
    ```sh
    Select max(col1) from table_name
    ```
* Select/project with distinct from one table:(distinct of  apir of values indicates the pair should be distinct)
    ```sh
    Select distinct col1,col2 from table_name
    ```
* Select with WHERE from one or more tables:
    ```sh
    Select col1,col2 from table1,table2 where col1=10 AND col2=20
    ```
* Select/project Columns(could be any number of columns) from able using "group by":
    ```sh
    Select col1, COUNT(col2) from table_name roup by col1
    ```
* Select/Project Columns from table in ascending/descending order according yo a column using "order by"
    ```sh
    Select col1,col2 from table_name order by col1 ASC|DESC"
    ```
    
    
#### Sample queries

   
    python3 db.py "select max(A),max(B) from table1;"
    python3 db.py "select count(*) from table1;"
    python3 db.py "select count(A) from table1;"
    python3 db.py "select * from table1,table2;"
    python3 db.py "select * from table1,table2 where A=B;"
    python3 db.py "select count(*) from table1,table2;"
    python3 db.py "select distinct A,D from table1,table2;"
    python3 db.py "select distinct A,B from table1,table2 where A>0 and B>0;"
    python3 db.py "select count(*) from table1,table2 where A>0 and B>0;"
    python3 db.py "select A,max(C) from table1,table2 group by A;"
    python3 db.py "select A,count(B) from table1,table2 group by A;"
    python3 db.py "select A,avg(B) from table1,table2 group by A order by A;"
    
