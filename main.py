import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connected to {db_file}, sqlite version: {sqlite3.version}")
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_project(conn, project):
    sql = 'INSERT INTO projects(nazwa, start_date, end_date) VALUES(?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()

    return cur.lastrowid


def add_task(conn, task):
    sql = 'INSERT INTO tasks (PROJEKT_ID, NAZWA, OPIS, STATUS, START_DATE, END_DATE) VALUES (?,?,?,?,?,?)'
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()

    return cur.lastrowid


def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    return rows


def select_where(conn, table, **query):
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    sql = f"SELECT * FROM {table} WHERE {q}"
    cur.execute(sql, values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")

if __name__ == '__main__':
    create_project_table = ("CREATE TABLE projects "
                            "(ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                            "NAZWA TEXT,"
                            "START_DATE DATE, "
                            "END_DATE DATE)")

    create_task_table = ("CREATE TABLE tasks ("
                         "ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
                         "PROJEKT_ID INTEGER NOT NULL,"
                         "NAZWA TEXT,"
                         "OPIS TEXT,"
                         "STATUS TEXT,"
                         "START_DATE DATE,"
                         "END_DATE DATE,"
                         "FOREIGN KEY(PROJEKT_ID) REFERENCES Projekt(ID)"
                         ")")

    connection = create_connection(r"database.db")
    with connection:
        execute_sql(connection, create_project_table)
        execute_sql(connection, create_task_table)

        delete_all(connection, "projects")
        delete_all(connection, "tasks")

        python_project_id = add_project(connection, ("Nauka python", '2024-06-01', '2024-09-30'))

        add_task(connection, (python_project_id, "PODSTAWY", "NAUKA PODSTAW", "DONE", '2024-06-01', '2024-06-31'))
        add_task(connection, (python_project_id, "ZAAWANSOWANE", "NAUKA ZAAWANSOWANE", "DONE", '2024-07-01', '2024-08-31'))
        add_task(connection, (python_project_id, "PODSUMOWANIE", "PODSUMOWANIE ZDOMYTEJ WIEDZY", "TO_DO", '2024-09-01', '2024-09-31'))

        javascript_project_id = add_project(connection, ("Nauka Javascript", '2024-10-01', '2024-12-30'))

        add_task(connection, (javascript_project_id, "PODSTAWY", "NAUKA PODSTAW", "TO_DO", '2024-10-01', '2024-10-30'))
        add_task(connection, (javascript_project_id, "ZAAWANSOWANE", "NAUKA ZAAWANSOWANE", "TO_DO", '2024-11-01', '2024-11-30'))
        add_task(connection, (javascript_project_id, "PODSUMOWANIE", "PODSUMOWANIE ZDOMYTEJ WIEDZY", "TO_DO", '2024-12-01', '2024-12-30'))

        java_project_id = add_project(connection, ("Nauka JAVA", '2025-01-01', '2024-03-30'))

        add_task(connection, (java_project_id, "PODSTAWY", "NAUKA PODSTAW", "TO_DO", '2025-01-01', '2025-01-31'))
        add_task(connection, (java_project_id, "ZAAWANSOWANE", "NAUKA ZAAWANSOWANE", "TO_DO", '2025-02-01', '2025-02-27'))
        add_task(connection, (java_project_id, "PODSUMOWANIE", "PODSUMOWANIE ZDOMYTEJ WIEDZY", "TO_DO", '2025-03-01', '2025-03-30'))

        print(select_all(connection, "projects"))
        print(select_all(connection, "tasks"))

        update(connection, "tasks", id=python_project_id, NAZWA="ZAAWANSOWANE", STATUS="DONE")
        update(connection, "tasks", id=java_project_id, NAZWA="ZAAWANSOWANE", STATUS="DONE")
        update(connection, "tasks", id=javascript_project_id, NAZWA="ZAAWANSOWANE", STATUS="DONE")

        print(select_where(conn=connection, table="tasks", PROJEKT_ID=python_project_id))

        delete_where(connection, "projects", NAZWA="Nauka JAVA")

        print(select_all(conn=connection, table="projects"))