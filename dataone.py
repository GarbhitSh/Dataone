import pickle
import os
import zlib
from typing import Dict, List
from datetime import datetime

# Constants for data types
INT = 1
STRING = 2
FLOAT = 3
DATE = 4
BOOLEAN = 5
CHAR = 6
TEXT = 7

def convert_value(value, data_type):
    if data_type == INT:
        return int(value)
    elif data_type == FLOAT:
        return float(value)
    elif data_type == BOOLEAN:
        return value.lower() == 'true'
    elif data_type == DATE:
        return datetime.strptime(value, "%Y-%m-%d")  # Format YYYY-MM-DD
    elif data_type == CHAR:
        return value[0] if len(value) > 0 else ''
    elif data_type == TEXT or data_type == STRING:
        return str(value)
    else:
        raise ValueError(f"Unsupported data type: {data_type}")

class Record:
    def __init__(self, data: Dict[str, any], schema: Dict[str, int]):
        self.data = {}
        for col_name, data_type in schema.items():
            if col_name in data:
                self.data[col_name] = convert_value(data[col_name], data_type)
            else:
                self.data[col_name] = None  # Default value if missing

    def __str__(self):
        return str(self.data)

    def __getitem__(self, key):
        return self.data.get(key)

    def __setitem__(self, key, value):
        self.data[key] = value

class Table:
    def __init__(self, name: str, schema: Dict[str, int], primary_key: str = None):
        self.name = name
        self.schema = schema
        self.records = {}
        self.primary_key = primary_key
        self.foreign_keys = []

    def insert(self, record: Record):
        if self.primary_key:
            pk_value = record[self.primary_key]
            if pk_value in self.records:
                raise ValueError(f"Primary key {pk_value} already exists")
            self.records[pk_value] = record
        else:
            raise ValueError("Cannot insert without a primary key")

    def update(self, record_id: any, updated_record: Dict[str, any]):
        if self.primary_key is None:
            raise ValueError("Cannot update without a primary key")
        
        record_id = convert_value(record_id, self.schema[self.primary_key])
        if record_id not in self.records:
            raise ValueError(f"Record with primary key {record_id} not found")
        
        for key, value in updated_record.items():
            if key in self.schema:
                self.records[record_id][key] = convert_value(value, self.schema[key])

    def delete(self, record_id: any):
        if self.primary_key is None:
            raise ValueError("Cannot delete without a primary key")
        
        record_id = convert_value(record_id, self.schema[self.primary_key])
        if record_id not in self.records:
            raise ValueError(f"Record with primary key {record_id} not found")
        
        del self.records[record_id]

    def query(self, conditions: Dict[str, any]) -> List[Record]:
        result = []
        for record in self.records.values():
            if all(record[k] == v for k, v in conditions.items()):
                result.append(record)
        return result

    def join(self, other_table, on_column):
        result = []
        for record in self.records.values():
            for other_record in other_table.records.values():
                if record[on_column] == other_record[on_column]:
                    merged_data = {**record.data, **other_record.data}
                    result.append(merged_data)
        return result

    def __str__(self):
        return f"Table: {self.name}, Records: {len(self.records)}\n" + "\n".join(str(record) for record in self.records.values())

class Database:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.tables = {}
        self.transactions = []

    def create_table(self, table_name: str, schema: Dict[str, int], primary_key: str = None):
        if table_name in self.tables:
            raise ValueError(f"Table {table_name} already exists")
        table = Table(table_name, schema, primary_key)
        self.tables[table_name] = table

    def get_table(self, table_name: str):
        return self.tables.get(table_name)

    def begin_transaction(self):
        self.transactions.append([])

    def commit_transaction(self):
        if self.transactions:
            self.transactions.pop()

    def rollback_transaction(self):
        if self.transactions:
            self.transactions.pop()

    def store(self):
        with open(self.db_name, 'ab') as f:
            f.seek(0, 2)  # Move cursor to the end of the file
            data = pickle.dumps(self)
            compressed_data = zlib.compress(data)
            f.write(compressed_data)

    @staticmethod
    def load(db_name: str):
        if os.path.exists(db_name):
            with open(db_name, 'rb') as f:
                f.seek(0)  # Move cursor to the beginning of the file
                compressed_data = f.read()
                data = zlib.decompress(compressed_data)
                return pickle.loads(data)
        return None

    def execute(self, query: str):
        print("-----------------------------------------------")
        result = parse_and_execute_query(self, query)
        self.store()  # Store the database after each query
        return result

def parse_and_execute_query(db, query: str):
    tokens = query.split()
    action = tokens[0].lower()

    if action == 'build':
        return _create_table(db, tokens[1:])
    elif action == 'add':
        if len(tokens) > 1 and tokens[1].lower() == 'in':
            return _insert_record(db, tokens[2:])
        else:
            raise ValueError("Invalid 'add' command. Use 'add in <table_name> ...'")
    elif action == 'change':
        return _update_record(db, tokens[1:])
    elif action == 'kick':
        if tokens[1].lower() == 'out':
            return _delete_record(db, tokens[2:])
        else:
            raise ValueError("Invalid 'kick' command. Use 'kick out <table_name> ...'")
    elif action == 'mix':
        if tokens[1].lower() == 'it' and tokens[2].lower() == 'up':
            return _join_tables(db, tokens[3:])
        else:
            raise ValueError("Invalid 'mix' command. Use 'mix it up <table1> <table2> <column>'")
    elif action == 'show':
        return _show_table(db, tokens[1:])
    else:
        raise ValueError(f"Unknown action: {action}")

def _create_table(db, tokens):
    if len(tokens) < 3:
        raise ValueError("Invalid 'build' command. Use 'build <table_name> <column1> <type1> <column2> <type2> ... [primarykey <column_name>]'")
    
    table_name = tokens[0]
    schema = {}
    primary_key = None
    i = 1
    while i < len(tokens):
        if tokens[i].lower() == 'primarykey':
            if i + 1 < len(tokens):
                primary_key = tokens[i + 1]
                break
            else:
                raise ValueError("Missing primary key column name")
        
        if i + 1 >= len(tokens):
            raise ValueError(f"Missing type for column {tokens[i]}")
        
        col_name = tokens[i]
        col_type = tokens[i + 1].lower()
        
        if col_type == 'int':
            schema[col_name] = INT
        elif col_type == 'float':
            schema[col_name] = FLOAT
        elif col_type == 'boolean':
            schema[col_name] = BOOLEAN
        elif col_type == 'date':
            schema[col_name] = DATE
        elif col_type == 'char':
            schema[col_name] = CHAR
        elif col_type == 'text':
            schema[col_name] = TEXT
        else:
            schema[col_name] = STRING
        
        i += 2
    
    db.create_table(table_name, schema, primary_key)
    return f"Table {table_name} created!"

def _insert_record(db, tokens):
    if len(tokens) < 3:
        raise ValueError("Invalid 'add' command. Use 'add in <table_name> <column1> <value1> <column2> <value2> ...'")
    
    table_name = tokens[0]
    table = db.get_table(table_name)
    if not table:
        raise ValueError(f"Table {table_name} not found!")
    
    record_data = {}
    i = 1
    while i < len(tokens):
        if i + 1 >= len(tokens):
            raise ValueError(f"Missing value for column {tokens[i]}")
        
        col_name = tokens[i]
        value = tokens[i + 1]
        
        # Handle quoted strings
        if value.startswith("'") or value.startswith('"'):
            j = i + 2
            while j < len(tokens) and not (tokens[j-1].endswith("'") or tokens[j-1].endswith('"')):
                value += " " + tokens[j]
                j += 1
            if j == len(tokens) and not (value.endswith("'") or value.endswith('"')):
                raise ValueError(f"Unclosed quote in value for column {col_name}")
            value = value[1:-1]  # Remove quotes
            i = j
        else:
            i += 2
        
        record_data[col_name] = value
    
    record = Record(record_data, table.schema)
    table.insert(record)
    return f"Record added to {table_name}!"

def _update_record(db, tokens):
    if len(tokens) < 4:
        raise ValueError("Invalid 'change' command. Use 'change <table_name> <record_id> <column1> <value1> [<column2> <value2> ...]'")
    
    table_name = tokens[0]
    record_id = tokens[1]
    table = db.get_table(table_name)
    if not table:
        raise ValueError(f"Table {table_name} not found!")
    
    updated_data = {}
    i = 2
    while i < len(tokens):
        if i + 1 >= len(tokens):
            raise ValueError(f"Missing value for column {tokens[i]}")
        
        col_name = tokens[i]
        value = tokens[i + 1]
        
        # Handle quoted strings
        if value.startswith("'") or value.startswith('"'):
            j = i + 2
            while j < len(tokens) and not (tokens[j-1].endswith("'") or tokens[j-1].endswith('"')):
                value += " " + tokens[j]
                j += 1
            if j == len(tokens) and not (value.endswith("'") or value.endswith('"')):
                raise ValueError(f"Unclosed quote in value for column {col_name}")
            value = value[1:-1]  # Remove quotes
            i = j
        else:
            i += 2
        
        updated_data[col_name] = value
    
    table.update(record_id, updated_data)
    return f"Record {record_id} updated in {table_name}!"

def _delete_record(db, tokens):
    if len(tokens) != 2:
        raise ValueError("Invalid 'kick out' command. Use 'kick out <table_name> <record_id>'")
    
    table_name = tokens[0]
    record_id = tokens[1]
    table = db.get_table(table_name)
    if not table:
        raise ValueError(f"Table {table_name} not found!")
    
    table.delete(record_id)
    return f"Record {record_id} removed from {table_name}!"

def _join_tables(db, tokens):
    if len(tokens) != 3:
        raise ValueError("Invalid 'mix it up' command. Use 'mix it up <table1> <table2> <column>'")
    
    table_name_1 = tokens[0]
    table_name_2 = tokens[1]
    on_column = tokens[2]
    
    table_1 = db.get_table(table_name_1)
    table_2 = db.get_table(table_name_2)
    
    if not table_1:
        raise ValueError(f"Table {table_name_1} not found!")
    if not table_2:
        raise ValueError(f"Table {table_name_2} not found!")
    
    result = table_1.join(table_2, on_column)
    return result

def _show_table(db, tokens):
    if len(tokens) != 1:
        raise ValueError("Invalid 'show' command. Use 'show <table_name>'")
    
    table_name = tokens[0]
    table = db.get_table(table_name)
    if not table:
        raise ValueError(f"Table {table_name} not found!")
    
    return str(table)

if __name__ == "__main__":
    db = Database("dataone.db")
    loaded_db = Database.load("dataone.db")
    if loaded_db:
        print("Database loaded successfully")
        db = loaded_db
        print(db.execute("show users"))
    else:
        print("No existing database found. Starting with a new database.")
    print("DataOne Database is ready. Enter your queries (type 'bye' to exit):")
    query = input()
    while query.lower() != 'bye':
        try:
            result = db.execute(query)
            print(result)
        except Exception as e:
            print(f"Error: {str(e)}")
        print("-----------------------------------------------")

        query = input()

    print("Thank you for using DataOne Database. Goodbye!")

