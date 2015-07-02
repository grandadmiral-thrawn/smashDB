import unittest
import datetime
import smashControls
import smashWorkers
import smashBosses

class testMainLoop(unittest.TestCase):

    def test_basic_airtemp(self):
        A = smashWorkers.AirTemperature('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Air Temp is a list"
        del A

    def test_longterm_airtemp(self):
        A = smashWorkers.AirTemperature('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Air Temp over 2 + years is a list"
        del A

    def test_sheldon_airtemp(self):
        A = smashWorkers.AirTemperature('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from airtemp, SHELDON is a list"
        del A

    def test_basic_relhum(self):
        A = smashWorkers.RelHum('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from RelHum is a list"
        del A

    def test_longterm_relhum(self):
        A = smashWorkers.RelHum('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from RelHum over 2 + years, is a list"
        del A

    def test_sheldon_relhum(self):
        A = smashWorkers.RelHum('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from RelHum, SHELDON is a list"
        del A

    def test_basic_dewpoint(self):
        A = smashWorkers.DewPoint('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from DewPoint is a list"
        del A

    def test_longterm_dewpoint(self):
        A = smashWorkers.DewPoint('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Dewpoint, SHELDON is a list"
        del A

    def test_sheldon_dewpoint(self):
        A = smashWorkers.DewPoint('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Dewpoint, SHELDON is a list"
        del A

    def test_basic_vpd(self):
        A = smashWorkers.VPD2('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from VOD  is a list"
        del A

    def test_longterm_vpd(self):
        A = smashWorkers.VPD2('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from VPD over 2 + years is a list"
        del A

    def test_sheldon_vpd(self):
        A = smashWorkers.VPD2('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from VOD, SHELDON is a list"
        del A

    def test_basic_solar(self):
        A = smashWorkers.Solar('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Solar is a list"
        del A

    def test_longterm_solar(self):
        A = smashWorkers.Solar('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Solar over 2 + years  is a list"
        del A

    def test_sheldon_solar(self):
        A = smashWorkers.Solar('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Solar, SHELDON is a list"
        del A

    def test_basic_wind(self):
        A = smashWorkers.Wind('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Wind is a list"
        del A

    def test_longterm_wind(self):
        A = smashWorkers.Wind('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Wind over 2 + years  is a list"
        del A

    def test_sheldon_wind(self):
        A = smashWorkers.Wind('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Wind, SHELDON is a list"
        del A

    def test_basic_par(self):
        A = smashWorkers.PhotosyntheticRad('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from PhotosyntheticRad is a list"
        del A

    def test_longterm_par(self):
        A = smashWorkers.PhotosyntheticRad('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from PhotosyntheticRad over 2 + years  is a list"
        del A

    def test_sheldon_par(self):
        A = smashWorkers.PhotosyntheticRad('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from PhotosyntheticRad, SHELDON is a list"
        del A

    def test_basic_st(self):
        A = smashWorkers.SoilTemperature('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from ST is a list"
        del A

    def test_longterm_st(self):
        A = smashWorkers.SoilTemperature('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from ST over 2 + years  is a list"
        del A

    def test_sheldon_st(self):
        A = smashWorkers.SoilTemperature('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from ST, SHELDON is a list"
        del A

    def test_basic_sw(self):
        A = smashWorkers.SoilWaterContent('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from SWC is a list"
        del A

    def test_longterm_sw(self):
        A = smashWorkers.SoilWaterContent('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from SWC over 2 + years  is a list"
        del A

    def test_sheldon_sw(self):
        A = smashWorkers.SoilWaterContent('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from SWC, SHELDON is a list"
        del A

    def test_basic_pre(self):
        A = smashWorkers.Precipitation('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from pre is a list"
        del A

    def test_longterm_pre(self):
        A = smashWorkers.Precipitation('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from pre over 2 + years  is a list"
        del A

    def test_sheldon_pre(self):
        A = smashWorkers.Precipitation('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from pre, SHELDON is a list"
        del A

    def test_basic_sn(self):
        A = smashWorkers.SnowLysimeter('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from snow is a list"
        del A

    def test_longterm_sn(self):
        A = smashWorkers.SnowLysimeter('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from snow over 2 + years  is a list"
        del A

    def test_sheldon_sn(self):
        A = smashWorkers.SnowLysimeter('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from snow, SHELDON is a list"
        del A

    def test_basic_nr(self):
        A = smashWorkers.NetRadiometer('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from NetRadiometer is a list"
        del A

    def test_sheldon_nr(self):
        A = smashWorkers.NetRadiometer('2015-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from NetRadiometer, SHELDON is a list"
        del A

    def test_basic_sonic(self):
        A = smashWorkers.Sonic('2013-01-01 00:00:00', '2013-01-05 00:00:00', 'STEWARTIA' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Sonic is a list"
        del A

    def test_sheldon_sonic(self):
        A = smashWorkers.Sonic('2015-01-01 00:00:00', '2015-01-05 00:00:00', 'SHELDON' )
        nr = A.condense_data()
        self.assertIsInstance(nr, list)
        print "Testing that the data returned from Sonic, SHELDON is a list"
        del A


class VPDTest(unittest.TestCase):

    def test_VPD2(self):
        A = smashWorkers.VPD2('2013-01-01 00:00:00', '2015-01-05 00:00:00', 'STEWARTIA')
        if not A.od.keys():
            self.assertFalse()

# if __name__ == "__main__":
#     testSuite = unittest.TestLoader().loadTestsFromTestCase(testMainLoop)
#     # def testWorkers(self):
#     #     A = smashWorkers.AirTemperature(self.sd, self.edu, self.server)
#     #     nr = A.condense_data()
#     #     assertIsInstance(nr, )




if __name__ == "__main__":
    unittest.main()

            
