"""Some util functions."""
import hashlib

def generate_code(s):
  """Generates a CST entry code."""
  return "table_" + hashlib.md5(s).hexdigest()

def make_table_name(d):
  """Gets a table name from a list of keys."""
  s = "".join(sorted(d))
  return generate_code(s)

def subset(l1, l2):
	return set(l1).issubset(set(l2))

def disjoin(l1, l2):
	return len(set(l1) & set(l2)) == 0
