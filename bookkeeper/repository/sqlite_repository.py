from dataclasses import dataclass
from datetime import datetime
from inspect import get_annotations
from bookkeeper.repository.abstract_repository import AbstractRepository, T
import sqlite3
from typing import Any


@dataclass
class Test:
    name: str
    pk: int = 0
    

class SQLiteRepository(AbstractRepository[T]):
    def __init__(self, db_file: str, cls: type) -> None:
        self.db_file = db_file
        self.table_name = cls.__name__.lower()
        self.fields = get_annotations(cls, eval_str=True)
        self.fields.pop('pk')
        self.class_type = cls
        names = ', '.join(self.fields.keys())
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(f'CREATE TABLE IF NOT EXISTS {self.table_name} (idx INTEGER PRIMARY KEY, {names})')
            con.commit()

    def create_object(self, result: Any) -> T:
        obj: T = self.class_type()
        obj.pk = result[0]
        for x, res in zip(self.fields, result[1:]):
            setattr(obj, x, res)
        return obj

    def add(self, obj) -> int:
        if getattr(obj, 'pk', None) != 0:
            raise ValueError(f'trying to add object {obj} with filled \'pk\' attribute')
        names = ', '.join(self.fields.keys())
        placeholders = ', '.join("?" * len(self.fields))
        values = [getattr(obj, x) for x in self.fields]
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign keys = ON')
            cur.execute(f'INSERT INTO {self.table_name} ({names})  VALUES ({placeholders})', values)
            obj.pk = cur.lastrowid
        con.close()
        return obj.pk

    def get(self, pk: int) -> T | None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(f'SELECT * FROM {self.table_name} WHERE (idx = {pk})')
            result = cur.fetchone()
            if result is None:
                return None
            obj: T = self.create_object(result)
        con.close()
        return obj

    def filling(self, objbad: Any) -> list[type] | None:
        if len(objbad) == 0:
            return None

        elif len(objbad) == 1:

            objbad = objbad[0]
            objgood = self.class_type()
            temp_name = get_annotations(self.class_type, eval_str=True)
            names = tuple(temp_name.keys())
            for j in range(len(names)-1):
                setattr(objgood, names[j], objbad[j+1])
            setattr(objgood, 'pk', objbad[0])
            print(f'Сформирован: {str(objgood)}')
            return [objgood]
        else:
            temp_name = get_annotations(self.class_type, eval_str=True)
            names = tuple(temp_name.keys())
            arr: list[type] = []
            for k in range(len(objbad)):
                objbad_temp = objbad[k]
                objgood = self.class_type()
                for j in range(len(names) - 1):
                    setattr(objgood, names[j], objbad_temp[j + 1])
                setattr(objgood, 'pk', objbad_temp[0])
                arr = arr + [objgood]
            print(f'Сформированы: {str(arr)}')
            return arr

    def get_all(self, where: dict[str, Any] | None = None) -> list[T]:
        if where is None:
            with sqlite3.connect(self.db_file) as con:
                cur = con.cursor()
                cur.execute('PRAGMA foreign_keys = ON''PRAGMA foreign_keys = ON')
                cur.execute(f'SELECT * FROM {self.table_name}')
                obj = cur.fetchall()
                con.commit()
        else:
            columns_names = where.keys()
            condition = [f'{name} {where.get(name)}' for name in columns_names]
            condition = tuple(condition)
            join_cond = ' AND '.join(condition)

            with sqlite3.connect(self.db_file) as con:
                cur = con.cursor()
                cur.execute('PRAGMA foreign_keys = ON''PRAGMA foreign_keys = ON')
                cur.execute(f'SELECT * FROM {self.table_name} WHERE ({join_cond})')
                obj = cur.fetchall()
                con.commit()
                
        print(f'Получены объекты {obj}')

        return self.filling(obj)

    def update(self, obj: T) -> None:
        pk = obj.pk
        if self.get(pk) is None:

            raise ValueError(f'No object with idx = {obj.pk} in DB.')
        names = tuple(self.fields.keys())

        values = [getattr(obj, x) for x in self.fields]

        update_data = [f'{names[j]} = ?' for j in range(len(self.fields))]

        update_data = tuple(update_data)
        update_zapros = ', '.join(update_data)

        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(f'UPDATE {self.table_name} SET {update_zapros} WHERE (idx = {pk})', values)
            con.commit()
        

    def delete(self, pk: int) -> None:
        if self.get(pk) is None:

            raise KeyError(f'No object with idx = {pk} in DB.')
        
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(f'DELETE FROM {self.table_name} WHERE (idx = {pk})')
            con.commit()
        print(f'Запись удалена')
        
    def delete_all(self) -> None:
        with sqlite3.connect(self.db_file) as con:
            cur = con.cursor()
            cur.execute('PRAGMA foreign_keys = ON')
            cur.execute(f'DELETE FROM {self.table_name}')
            con.commit()
        print(f'Все записи удалены')
            
            
        
