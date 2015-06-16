import unittest
import datetime
import smashControls
import smashWorkers
import smashBosses


class testDateRange(unittest.TestCase):
    def runTest(self):

        sd = '2013-01-01 00:00:00'
        ed = '2014-01-01 00:00:10'

        A = smashWorkers.daterange(sd, ed)

        assert isinstance(A.dr[0], datetime)


class testDataRange(unittest.TestCase):

    sd = '1813-01-01 00:00:00'
    ed = '1814-01-01 00:00:10'
        
    def test1(self):
        A = smashWorkers.AirTemperature(sd, ed, 'SHELDON')
        self.assertRaises(ValueError, A)
        



class DateCleaner(object):

    def __init__(self, some_string):
        self.dater = some_string
        self.dateobj = datemagic(self.dater)

    def datemagic(self):
        try:
            # FSDB
            dt = datetime.datetime.strptime(self.dater,'%Y-%m-%d %H:%M:%S')
        except Exception:
            try:
                # METDAT
                dt = datetime.datetime.strptime(self.dater.rstrip('0').rstrip('.'),'%Y-%m-%d %H:%M:%S'
            except Exception:
                try:
                    dt = datetime.datetime.strptime(self.dater,'%m/%d/%y %H:%M')
                except Exception:
                    print "the test is bad"


if __name__ == "__main__":
    unittest.main()

            
