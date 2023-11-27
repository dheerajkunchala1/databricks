# Databricks notebook source
def reverse(s):
  return s[::-1]

# COMMAND ----------

import unittest

class TestHelpers(unittest.TestCase):
    def test_reverse(self):
        self.assertEqual(reverse('abc'), 'cda')


# COMMAND ----------

r = unittest.main(argv=[''], verbosity=3, exit=False)

# COMMAND ----------

assert r.result.wasSuccessful(), 'Test failed; see logs above'

# COMMAND ----------


