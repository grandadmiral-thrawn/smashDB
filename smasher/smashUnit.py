import unittest
from smashControls import *
from smashWorkers import *
from smashBosses import *

class DBControlTestCase(unittest.TestCase):

    def setUp(self):
        self.Controller = DBControl('SHELDON')
        self.Controller.build_queries()

class DBControlFSDBTestCase(unittest.TestCase):

    def setUp(self):
        self.Controller = DBControl('STEWARTIA')
        self.Controller.build_queries()

class DBControlStationTestCase(unittest.TestCase):

    def setUp(self):
        self.Controller = DBControl('SHELDON','PRIMET')
        self.Controller.build_queries_station()

class DBControlFSDBStationTestCase(unittest.TestCase):

    def setUp(self):
        self.Controller = DBControl('STEWARTIA','H15MET')
        self.Controller.build_queries_station()

class DBControlAttributesTestCase(DBControlTestCase):
    
    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("RELHUM")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PRECIP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_PRO")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_SNC")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("SOILTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("NR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PAR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("DEWPT")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("VPD")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

class DBControlStationFailTestCase(DBControlStationTestCase):

    @unittest.expectedFailure
    def test_fail(self):
        self.__setattr__('station','H15MET')
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Probe found taht doesn't actually have telemetry")

class DBControlStationAttributesTestCase(DBControlStationTestCase):
    
    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("AIRTEMP")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("RELHUM")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PRECIP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_PRO")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_SNC")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("SOILTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("NR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PAR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("DEWPT")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("VPD")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

class DBControlFSDBStationAttributesTestCase(DBControlFSDBStationTestCase):
    
    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("AIRTEMP")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("RELHUM")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PRECIP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_PRO")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_SNC")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("SOILTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("NR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PAR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("DEWPT")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

    def runTest(self):
        try:
            result = self.Controller.check_out_one_attribute("VPD")
            self.assertIsInstance(result, tuple, "Returned value is not a tuple")
        except KeyError:
            @unittest.skip("Skipping site already in table")
            def test_skip_me(self):
                self.fail("shouldn't happen")

class DBControlAttributesTestCase(DBControlTestCase):
    
    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("RELHUM")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PRECIP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_PRO")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_SNC")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("SOILTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("NR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PAR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("DEWPT")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("VPD")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

class DBControlFSDBAttributesTestCase(DBControlFSDBTestCase):
    
    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("RELHUM")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("AIRTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PRECIP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_PRO")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("WSPD_SNC")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("SOILTEMP")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("NR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("PAR")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("DEWPT")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

    def runTest(self):
        result = self.Controller.check_out_one_attribute("VPD")
        self.assertIsInstance(result, tuple, "Returned value is not a tuple")

class UpdateBossTestCaseSheldon(unittest.TestCase):

    def setUp(self):
        self.Boss = NetRadiometer('2015-01-01 00:00:00','2015-01-10 00:00:00','SHELDON')

class UpdateBossRowsTest(UpdateBossTestCaseSheldon):

    def runTest(self):
        """ Net Radiometer"""
        self.assertEqual(self.entity,'25')


if __name__ == '__main__':

    print "running unit tests for smasher-- smashControls-- used to tell if there is indexing errors. not completed yet."
    unittest.main()