# -*- coding: utf-8 -*-
"""
~~~~~~~~~~~~~~~~~~~~~~
databases db module

@author guoweikuang
"""
import time
import threading
import logging
import functools

import pymysql

engine = None


def create_engine(username, password, database, host, port, **kwargs):
    """create a database connection.

    :param username: user database name
    :param password: user database password
    :param database: database name
    :param host: host
    :param port: port
    :param kwargs: other params
    :return: None
    """
    global engine
    if engine is not None:
        raise DBError("Engine is already initialized")

    params = dict(user=username,
                  passwd=password,
                  db=database,
                  host=host,
                  port=port)
    default_params = dict(charset="utf8mb4", use_unicode="True")
    for k, v in default_params.items():
        params[k] = kwargs.pop(k, v)
    params.update(kwargs)
    engine = _Engine(lambda: pymysql.connect(**params))


class _Engine(object):
    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


class _Dbctx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not (self.connection is None)

    def init(self):
        self.connection = _LazyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()


_db_ctx = _Dbctx()


class _LazyConnection(object):
    """lazy load connection."""

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            logging.info('Connection OPEN is <%s>' % hex(id(_connection)))
            self.connection = _connection
        return self.connection.cursor()

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection
            self.connection = None
            logging.info("Connection CLOSE is <%s>" % hex(id(_connection)))
            _connection.close()


class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()


def connection():
    return _ConnectionCtx()


def _profiling(start, sql=''):
    """
    用于剖析sql的执行时间
    """
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))


def with_connection(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        with _ConnectionCtx():
            return func(*args, **kwargs)
    return _wrapper


def with_transaction(func):
    @functools.wraps(func)
    def _wrapper(*args, **kwargs):
        start = time.time()
        with _Transactions():
            return func(*args, **kwargs)
        _profiling(start)
    return _wrapper


class _Transactions(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()

        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()

    def rollback(self):
        global _db_ctx
        _db_ctx.connection.rollback()


class DBError(Exception):
    pass


class Field(dict):
    """Implement a simple dictionary that can be accessed through properties.
    like: x.key = value
    """
    def __init__(self, keys=set(), values=set(), **kwargs):
        super(Field, self).__init__(**kwargs)
        for k, v in zip(keys, values):
            self[k] = v

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AssertionError(r"'Field' object has not attribute '%s'" % item)

    def __setattr__(self, key, value):
        self[key] = value


@with_connection
def _select(sql, one, *args, **kwargs):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info("SQL: %s, args: %s", sql, args)

    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        if cursor.description:
            names = [field[0] for field in cursor.description]
        if one:
            values = cursor.fetchone()
            if not values:
                return None
            return Field(keys=names, values=values)
        return [Field(keys=names, values=values) for values in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


def select_one(sql, *args, **kwargs):
    """ return one sql data.

    :param sql:
    :param args:
    :param kwargs:
    :return:
    """
    return _select(sql, True, *args, **kwargs)

def select(sql, *args, **kwargs):
    """execute sql to get datas.

    >>> select_sql = select('select * from users where id=?', 1)
    >>> select_sql[0].id
    1
    >>> select_sql[0].username
    guoweikuang
    """
    return _select(sql, False, *args, **kwargs)


@with_connection
def _update(sql, *args, **kwargs):
    """update data."""
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, args: %s', sql, args)

    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        row_count = cursor.rowcount
        if not _db_ctx.transactions:
            logging.info("auto commit")
            _db_ctx.connection.commit()
        return row_count
    finally:
        if cursor:
            cursor.close()


def update(sql, *args, **kwargs):
    """update data.

    :param sql:
    :param args:
    :param kwargs:
    :return:
    """
    return _update(sql, *args, **kwargs)


def insert(table_name, **kwargs):
    """insert new data to database.

    :param sql:
    :param args:
    :param kwargs:
    :return:
    """
    cols, args = zip(*kwargs.items())
    sql = "insert into `%s` (%s) values(%s)" % (table_name, ','.join(['`%s`' % col for col in cols]),
          ','.join(['?' for _ in range(len(cols))]))
    return _update(sql, *args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    create_engine(username='root', password='2014081029', database='test', host='localhost', port=3306)
    select_obj = select("select * from users where id=?", 2)
    print(select_obj[0].username)

    update_obj = update("update users set username=? where id=?", "kuang", 2)
    print(update_obj)

    user = dict(username="guowei")
    insert_obj = insert("users", **user)
    print(insert_obj)