import sqlite3
from typing import List, Dict, Any, Optional

class Database:
    def __init__(self, db_type: str, **kwargs):
        self.db_type = db_type
        self.conn = None
        self.param = '?'
        if db_type == 'sqlite':
            self.conn = sqlite3.connect(kwargs['database'])
        elif db_type == 'postgresql':
            import psycopg2
            self.conn = psycopg2.connect(
                host=kwargs.get('host'),
                database=kwargs.get('database'),
                user=kwargs.get('user'),
                password=kwargs.get('password')
            )
            self.param = '%s'
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def execute(self, sql: str, params=()) -> Any:
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params)
            self.conn.commit()
            return cursor
        except Exception as e:
            self.conn.rollback()
            raise e

    def create_table(self, table_name: str, schema: Dict[str, str]) -> None:
        columns = ', '.join([f'{col} {dtype}' for col, dtype in schema.items()])
        sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})'
        self.execute(sql)

    def drop_table(self, table_name: str) -> None:
        sql = f'DROP TABLE IF EXISTS {table_name}'
        self.execute(sql)

    def add_column(self, table_name: str, column_name: str, data_type: str) -> None:
        sql = f'ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}'
        self.execute(sql)

    def drop_column(self, table_name: str, column_name: str) -> None:
        if self.db_type == 'sqlite':
            raise NotImplementedError("SQLite does not support DROP COLUMN")
        else:
            sql = f'ALTER TABLE {table_name} DROP COLUMN {column_name}'
            self.execute(sql)

    def alter_column(self, table_name: str, column_name: str, new_data_type: str) -> None:
        if self.db_type == 'sqlite':
            raise NotImplementedError("SQLite does not support ALTER COLUMN")
        else:
            sql = f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_data_type}'
            self.execute(sql)

    def insert(self, table_name: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        columns = ', '.join(rows[0].keys())
        placeholders = ', '.join([self.param] * len(rows[0]))
        sql = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        cursor = self.conn.cursor()
        try:
            for row in rows:
                values = list(row.values())
                cursor.execute(sql, values)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def select(
        self,
        table_name: str,
        where: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        sql = f'SELECT * FROM {table_name}'
        params = []
        if where:
            conditions = []
            for condition in where:
                col, op, val = condition
                if op.upper() == 'IN':
                    if not isinstance(val, (list, tuple)):
                        raise ValueError("IN operator requires a list or tuple of values")
                    placeholders = ', '.join([self.param] * len(val))
                    conditions.append(f"{col} {op} ({placeholders})")
                    params.extend(val)
                else:
                    conditions.append(f"{col} {op} {self.param}")
                    params.append(val)
            sql += ' WHERE ' + ' AND '.join(conditions)
        if order_by:
            sql += f' ORDER BY {order_by}'
        if limit is not None:
            sql += f' LIMIT {self.param}'
            params.append(limit)
        cursor = self.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def update(self, table_name: str, updates: Dict[str, Any], where: Optional[List[tuple]] = None) -> None:

        update_clause = ', '.join([f"{col} = {self.param}" for col in updates.keys()])
        params = list(updates.values())
        sql = f"UPDATE {table_name} SET {update_clause}"
        if where:
            conditions = []
            for condition in where:
                col, op, val = condition
                if op.upper() == 'IN':
                    if not isinstance(val, (list, tuple)):
                        raise ValueError("IN operator requires a list or tuple of values")
                    placeholders = ', '.join([self.param] * len(val))
                    conditions.append(f"{col} IN ({placeholders})")
                    params.extend(val)
                else:
                    conditions.append(f"{col} {op} {self.param}")
                    params.append(val)
            sql += " WHERE " + " AND ".join(conditions)
        self.execute(sql, params)

    def delete(self, table_name: str, where: Optional[List[tuple]] = None) -> None:

        sql = f"DELETE FROM {table_name}"
        params = []
        if where:
            conditions = []
            for condition in where:
                col, op, val = condition
                if op.upper() == 'IN':
                    if not isinstance(val, (list, tuple)):
                        raise ValueError("IN operator requires a list or tuple of values")
                    placeholders = ', '.join([self.param] * len(val))
                    conditions.append(f"{col} IN ({placeholders})")
                    params.extend(val)
                else:
                    conditions.append(f"{col} {op} {self.param}")
                    params.append(val)
            sql += " WHERE " + " AND ".join(conditions)
        self.execute(sql, params)

    def close(self) -> None:
        self.conn.close()