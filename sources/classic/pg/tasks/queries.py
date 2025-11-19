from uuid import UUID

from psycopg import Connection


def register_tasks(conn: Connection, worker_id: UUID, *tasks: str) -> None:
    conn.execute('''
        SELECT register_tasks(%s, %s);
    ''', (worker_id, tasks))


def unregister_tasks(conn: Connection, worker_id: UUID, *tasks: str) -> None:
    conn.execute('''
        SELECT register_tasks(%s, %s);
    ''', (worker_id, tasks))


def listen(conn: Connection, channel: str) -> None:
    conn.execute(f'''
        LISTEN {channel};
    ''')


def unlisten(conn: Connection, channel: str) -> None:
    conn.execute(f'''
        UNLISTEN {channel}
    ''')


def get_tasks_batch(conn: Connection, limit: int) -> list[str]:
    return conn.execute(f'''
        SELECT * FROM tasks LIMIT %s
    ''', limit).fetchall()


def mark_tasks_as_finished(conn: Connection):
    conn.execute(f'''
        
    ''')


def mark_tasks_as_cancelled(conn: Connection):
    conn.execute(f'''

    ''')


def mark_tasks_as_need_retry(conn: Connection):
    conn.execute(f'''

    ''')
