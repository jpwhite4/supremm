#!/usr/bin/env python
"""
 Helper script that prints information about the installed SUPReMM
 processing software
"""

import logging
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.scripthelpers import setuplogger

class MockJob(object):
    """ Skeleton implementation of the job class to allow the plugins to be instantiated """
    def __init__(self):
        self.job_id = "1"
        self.nodecount = 1
        self.walltime = 1
        self.acct = {'uid': 1, "user": "x"}


def get_reqd_str(reqd, allmetrics):

    metriclist = []
    noalt = ""
    
    for metric in reqd:
        if isinstance(metric, str):
            noalt += "    {0}\n".format(metric)
            allmetrics[metric] = 1
        else:
            alternatives = ""
            for mm in metric:
                alternatives += "    {0}\n".format(mm)
                allmetrics[mm] = 1
            metriclist.append(alternatives)

    if noalt != "":
        metriclist.append(noalt)

    return "\n" + " or \n".join(metriclist)

def print_metrics():
    """ X """

    plugins = loadpreprocessors()
    plugins += loadplugins()

    mockjob = MockJob()

    allmetrics = {}

    for plugin in plugins:
        inst = plugin(mockjob)
        print plugin
        print get_reqd_str(inst.requiredMetrics, allmetrics)

    mkeys = allmetrics.keys()
    mkeys.sort()
    print "All metrics:\n"
    print "\n".join(mkeys)

def main():
    """
    main entry point for script
    """

    setuplogger(logging.INFO)

    print_metrics()

if __name__ == "__main__":
    main()
