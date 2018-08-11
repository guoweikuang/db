# -*- coding: utf-8 -*-
"""
~~~~~~~~~~~~~~~~~~~~~~~
db unit tests module

@author guoweikuang
"""
import unittest
import threading

import db
from db import create_engine, connection


class DBTestCase(unittest.TestCase):
    def setUp(self):
        create_engine(username='root',
                      password='2014081029',
                      database='test',
                      host='localhost',
                      port=3306)

    def tearDown(self):
        db.engine = None

    def test_engine_is_connnect(self):
        self.assertTrue(db.engine is not None)

    def test_thread_engine_is_different(self):
        pass

    def test_select(self):
        with connection():
            select_obj = db.select("select * from users where id=?", 2)
            self.assertTrue(select_obj[0].username == 'kuang')

    def test_insert(self):
        with connection():
            user = dict(username='guo')
            insert_obj = db.insert("users", **user)
            self.assertTrue(insert_obj == 1)

if __name__ == '__main__':
    unittest.main()