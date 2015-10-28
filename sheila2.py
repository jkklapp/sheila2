"""
    Main file where the Sheila class is defined.
"""

import sqlite3
import logging
import pickle
import json
import os

from cst import CodeTable
from util import make_table_name
from util import subset
from util import disjoin


class Sheila:
    _instance = None

    def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(Backend, cls).__new__(cls, *args, **kwargs)
		return cls._instance

    def __init__(self, dbname, cst_file):
    	self.logger = logging.getLogger("sheila")
    	self.logger.info("Connecting to DB...")
        self.dbname = dbname
        self.conn = sqlite3.connect(database=dbname)
    	self.logger.info("DB Online.")
        self.cst_file = cst_file
        self.cst = CodeTable(cst_file)

    def execute_sql_stmt(self, stmt):
        cursor = self.conn.cursor()
        try:
    		cursor.execute(stmt)
    		self.conn.commit()
    	except Exception as e:
            self.logger.critical(e)
            self.logger.critical(stmt)
            self.conn.rollback()

    def create_table(self, name, keys):
        cst = self.cst
        stmt = 'CREATE TABLE {t} ( data text )'.format(t=name)
    	self.execute_sql_stmt(stmt)
    	cst[name] = keys
    	pickle.dump(cst, open(self.cst_file,'w'))

    def updateTable(self, old, new):
    	'''Change table name '''
        cst = self.cst
        cst[old] = new
    	pickle.dump(cst, open(self.cst_file, 'w'))

    def query_match(self, f, d1, d2):
    	"""Develop for more complex queries."""
    	if f == 'equal':
    		return d1 == d2

    def sql_insert(self, data, table):
        data_as_string = "'" + json.dumps(data) + "'"
        stmt = "INSERT INTO {t} VALUES ({v})".format(t=table, v=data_as_string)
        self.execute_sql_stmt(stmt)

    def sql_select(self, data, table):
    	f = 'equal'
     	c = self.conn.cursor()
     	# current duplicate-free policy on queries
     	self.logger.info("MySQL select on "+table)
        stmt = 'SELECT data FROM {t}'.format(t=table)
     	try:
    		c.execute(stmt)
    	except:
    		self.logger.critical("Select on "+table)
    	r = []
    	for row in c.fetchall():
            j = json.loads(row[0])
            if len(j) < len(data):
                continue
            add = False
            for d in data.keys():
                try:
                    if self.query_match(f, data[d], j[d]):
                        add = True
                except KeyError:
                    continue
            if add:
        		r.append(j)
        self.logger.debug("Returned "+str(len(r))+" results from "+table)
        return r

    def destroy(self):
        os.remove(self.dbname)

    def insert(self, data):
        new = data.keys()
        cst = self.cst
        operation = 'Create'
    	# look for target table code
        for key in cst.tables():
            if subset(new, cst[key]) and len(new) <= len(cst[key]):
            	operation = 'Insert'
            	#target = cst[key]
            	tTag = cst.get_set_with_most_common_tags(new)
                tKeys = cst[tTag]
                target = tTag
                self.sql_insert(data, target)
                break
            elif not disjoin(new, cst[key]):
                tTag = cst.get_set_with_most_common_tags(new)
                tKeys = cst[tTag]
                if len(tTag) == 0 or len(tKeys) == 0:
                    continue
                operation = 'Update'
                new = list(set(new) | set(tKeys))
                target=tTag
                self.updateTable(target, new)
                self.sql_insert(data, tTag)
                break
        if operation == 'Create':
            target = make_table_name(new)
            self.create_table(target, new)
            self.sql_insert(data, target)

    def query(self, data):
        keys = data.keys()
        cst = self.cst
    	tKeys = cst.get_common_sets(keys)
    	if tKeys == []:
    		logger.debug("\nNo table for data: " + data)
    		return []
    	ret = []
    	for tTag in tKeys:
    		partial = self.sql_select(data, tTag)
    		for p in partial:
    			ret.append(p)
    	return ret
