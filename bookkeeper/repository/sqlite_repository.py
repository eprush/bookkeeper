"""
Модуль описывает репозиторий, работающий с помощью СУБД SQLite
"""
import sqlite3
from inspect import get_annotations
from typing import Any

from bookkeeper.repository.abstract_repository import AbstractRepository, T


class SQLiteRepository(AbstractRepository[T]):
    """
    Репозиторий, работающий с помощью СУБД SQLite
    """

    def __init__(self, db_file: str, cls: type):
        self.db_file = db_file
        self.content_class = cls
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop("pk")

        def which(type_data) -> str:
            types = {int : "INTEGER NOT NULL",
                     str : "TEXT NOT NULL",
                     (int | None) : "INTEGER"}
            return types[type_data]

        str_types = "".join(f",\n? {which(my_type)}" for my_type in self.fields.values())
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f'''
                        CREATE TABLE IF NOT EXISTS {self.table_name} (
                        pk INTEGER PRIMARY KEY {str_types} 
                        );''',
                        tuple(self.fields.keys())
            )
        con.close()

    def add(self, obj: T) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled `pk` attribute')

        names = ", ".join(self.fields.keys())
        p = ", ".join("?" * len(self.fields))
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            values = tuple(getattr(obj, attr) for attr in self.fields.keys())
            cur.execute("PRAGMA foreign_keys = ON")
            if values:
                cur.execute(
                    f"INSERT INTO {self.table_name} ({names}) VALUES ({p})",
                    values
                )
            else:
                cur.execute(f"INSERT INTO {self.table_name} DEFAULT VALUES")
            pk = cur.lastrowid if cur.lastrowid is not None else 0
        con.close()
        obj.pk = pk
        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute(f"SELECT * FROM {self.table_name} WHERE pk == ?;", (pk,))
            params = cur.fetchone()
            obj = self.content_class(*params) if params is not None else None
        con.close()
        # do I get an object with pk attr or not?
        # this is question about PRIMARY KEY
        return obj

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            if where is None:
                cur.execute(f"SELECT * FROM {self.table_name}")
            else:
                str_where = " AND ".join(f"{attr} = ? " for attr in where.keys())
                cur.execute(f"SELECT * FROM {self.table_name} WHERE {str_where}", tuple(where.values()))
            res = cur.fetchall()
        con.close()
        return [self.content_class(*item) for item in res]

    def update(self, obj: T) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            str_request = f"SELECT * FROM {self.table_name} WHERE pk == ?;"
            if has_pk := (cur.execute(str_request,(obj.pk,)).fetchone() is not None):
                if not len(self.fields.keys()):
                    return
                str_fields = ", ".join(f"{field} = ?" for field in self.fields.keys())
                values = tuple(getattr(obj, attr) for attr in self.fields.keys()) + (obj.pk,)
                cur.execute(f"UPDATE {self.table_name} SET {str_fields} WHERE pk == ?;", values)
        con.close()
        if has_pk:
            return
        raise ValueError('attempt to update object with unknown primary key')


    def delete(self, pk: int) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            str_request = f"SELECT * FROM {self.table_name} WHERE pk = ?"
            if has_pk := (cur.execute(str_request,(pk,)).fetchone() is not None):
                cur.execute(f"DELETE FROM {self.table_name} WHERE pk = ?", (pk, ))
        con.close()
        if has_pk:
            return
        raise KeyError
