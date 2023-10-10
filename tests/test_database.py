import unittest
from database import DBConnection

class TestDBConnection(unittest.TestCase):

    def test_singleton_instance(self):
        db1 = DBConnection.getInstance()
        db2 = DBConnection.getInstance()
        self.assertEqual(db1, db2)

    def test_get_engine(self):
        db = DBConnection.getInstance()
        engine = db.getEngine()
        self.assertIsNotNone(engine)

    def test_get_connection_string(self):
        db = DBConnection.getInstance()
        conn_str = db.getConnectionString()
        self.assertIsNotNone(conn_str)
        
    
    

if __name__ == '__main__':
    unittest.main()