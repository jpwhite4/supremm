""" XX """
import datetime
import unittest
from mock import patch, Mock
import supremm
from supremm.Job import Job
from supremm.preprocessors import TorqueProc

class TestSummarizeJob(unittest.TestCase):

    def setUp(self):
        confjob = {
            'job_id': '1',
            'acct': {
                'user': 'test'
                },
            'nodecount': 1,
            'walltime': 600,
            'rawarchives.return_value': iter([('nodename', ['archive1', 'archive2'])]),
            'end_datetime': datetime.datetime(2016,1,1),
            'jobdir': None
        }
        self.mockjob = Mock(spec=Job, **confjob)

    def test_cpusetmatching(self):
        """ XX """

        tp = TorqueProc.TorqueProc(self.mockjob)

        cpusets = [('cpuset:/torque/2125569.somecluster-name.host.tld', '2125569'),
                   ('cpuset:/torque/2125569', '2125569'),
                   ('cpuset:/torque/2125569[23]', '2125569[23]'),
                   ('cpuset:/torque/2125569[24].somecluster', '2125569[24]'),
                   ('cpuset:/torque/2125569/somethingelse', '2125569')]

        for cpusetstring in cpusets:
            result = tp.cgroupparser(cpusetstring[0])
            self.assertEqual((None, cpusetstring[1]), result)

if __name__ == '__main__':
    unittest.main()
