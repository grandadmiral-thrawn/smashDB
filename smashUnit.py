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


class DateCleaner(object):

    def __init__(self, some_string):
        self.dater = some_string
        self.dateobj

    def regexit(self):
     
        is_big_list = self.dater.split('/')
        if len(is_big_list) > 1:
            return is_big_list

        elif len(is_big_list) <= 1:
            is_big_list_2 = self.dater.split('-')
            if is_big_list_2 > 1:
                return is_big_list_2

            elif is_big_list_2 <= 1:
                import pdb; pdb.set_trace()

    def which_is_time(list_of_dt):
        time_obj = [x for x in list_of_dt if ':' in x][0]
        nto = time_obj[0].split(' ')
        (h, m, s) = nto[1].split(':')
        if int(nto[0]) > 31:
            year = int(nto[0])
        else:
            day = int(nto[0])

        year_obj = [x for x in list_of_dt if ':' not in x]

        
        return (h,m,s)

            
