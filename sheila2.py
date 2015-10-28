"""
    Main file where the Sheila class is defined.
"""

import sqlite3
import logging
import pickle
import json

from cst import CodeTable
from util import make_table_name
from util import subset
from util import disjoin

CREATE_TABLE_SQL_STRING = "CREATE TABLE %s ( id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT, data TEXT NOT NULL )"

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
    		self.logger.debug("Creating new " + name)
    		cursor.execute(stmt)
    		self.conn.commit()
    	except Exception as e:
    		self.logger.critical(e)
    		self.conn.rollback()

    def createTable(self, name, keys):
        cst = self.cst
    	self.execute_sql_stmt(CREATE_TABLE_SQL_STRING % name)
    	cst[name] = keys
    	pickle.dump(cst, open(self.cst_file,'w'))

    def updateTable(self, old, new):
    	'''Change table name '''
        cst = self.cst
        cst[old] = new
    	pickle.dump(cst, open(self.cst_file, 'w'))

    def sql_insert(self, data, table):
    	# Actual data insertion
        self.execute_sql_stmt(("INSERT INTO "+table+"(data) VALUES (%s)",(str(data))))

    def sql_select(self, data, table):
    	f = 'equal'
     	c = self.conn.cursor()
     	# current duplicate-free policy on queries
     	self.logger.debug("MySQL select on "+table)
     	try:
    		c.execute("SELECT DISTINCT data FROM "+table)
    	except:
    		self.logger.critical("Select on "+table)
    	numrows = c.rowcount
    	r = []
    	for x in xrange(0,numrows):
      		row = c.fetchone()
      		j = json.loads(row[0])
      		if len(j) < len(data):
      			continue
      		add = False
      		for d in data.keys():
      			try:
      				if queryMatch(f,data[d],j[d]):
      					add = True
      			except KeyError:
      				continue
      		if add:
      			r.append(j)
      	self.logger.debug("Returned "+str(len(r))+" results from "+table)
      	return r

    def destroy(self):
        self.execute_sql_stmt(("DROP DATABASE %s", (self.dbname)))

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
                self.sql_insert(json.dumps(data), target)
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
                self.sql_insert(json.dumps(data), tTag)
                break
        if operation == 'Create':
            target = make_table_name(new)
            self.createTable(target, new)
            self.sql_insert(json.dumps(data), target)
