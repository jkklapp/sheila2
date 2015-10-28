"""CodeTable class definition."""
import pickle
import logging

class CodeTable:
	"""Class implementing the table where keys are indexed."""
	_instance = None
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
		return cls._instance

	def __init__(self, cstfile):
		logging.getLogger("sheila")
		try:
			logging.info("Loading existing CST '" + cstfile + "'...")
			self.table = pickle.load(open(cstfile, 'r'))
			logging.info("Existing CST loaded.")
		except IOError:
			logging.warning("Loading empty CST!")
			self.table = {}

	def __call__(self):
		return self.table

	def __str__(self):
		r = "CST state\n"
		for key in self.tables():
			r += "key: " + key + " content: " + self.table[key] + "\n"
		r += "----------------------------------------"
		return r

	def __setitem__(self, key, item):
		self.table[key] = item

	def __getitem__(self, key):
		return self.table[key]

	def get_keys_as_set(self, tag):
		return set(self.table[tag])

	def tables(self):
		return self.table.keys()

	def get_set_with_most_common_tags(self, target_keys):
		"""Gets the table where the object should be inserted."""
		count = 0
		for cst_key in self.tables():
			common_keys = len(set(target_keys) & set(self.table[cst_key]))
			if count < common_keys:
				count = common_keys
				best_table = cst_key
		return best_table
