import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import spark_session,clean_text,valid_bio,join_athletes,average_stats

class CleaningTests(unittest.TestCase):
 @classmethod
 def setUpClass(cls): cls.spark=spark_session('FP cleaning checks')
 @classmethod
 def tearDownClass(cls):cls.spark.stop()
 def test_text_and_full_row_duplicates(self):
  df=self.spark.createDataFrame([('1','  Zoë\u00a0  O’Neill\n'),('1','Zoë O’Neill'),('2',None)],'athlete_id string, name string')
  rows=clean_text(df).dropDuplicates().orderBy('athlete_id').collect()
  self.assertEqual([(r.athlete_id,r.name) for r in rows],[('1','Zoë O’Neill'),('2',None)])
 def test_numeric_and_no_medal_groups(self):
  bio=self.spark.createDataFrame([('1','180','70','Female','UKR'),('2','NaN','70','Male','USA'),('3','180','Infinity','Male','USA'),('4','n/a','70','Male','USA'),('5',None,'70','Male','USA'),('6',' 170 ','60','Male','UKR')],'athlete_id string,height string,weight string,sex string,country_noc string')
  valid=valid_bio(bio)
  self.assertEqual(sorted(r.athlete_id for r in valid.collect()),['1','6'])
  events=self.spark.createDataFrame([('1','Run','','USA'),('1','Run','Gold','USA'),('6','Run',None,'USA'),('999','Run','Gold','USA')],'athlete_id string,sport string,medal string,country_noc string')
  joined=join_athletes(events,valid)
  self.assertEqual(joined.count(),3)
  stats=average_stats(joined).collect()
  self.assertEqual(len(stats),3)
  self.assertTrue(all(r.country_noc=='UKR' for r in stats))
  self.assertEqual(sum(r.medal is None for r in stats),2)
  self.assertTrue(all(r.event_count==1 for r in stats))
 def test_raw_numeric_nonbreaking_space(self):
  bio=self.spark.createDataFrame([('1','180','70\u00a0'),('2',' 170\u00a0','60'),('3','180','7 0')],'athlete_id string,height string,weight string')
  self.assertEqual(sorted(r.athlete_id for r in valid_bio(bio).collect()),['1','2'])

if __name__=='__main__':unittest.main(verbosity=2)
