#!/usr/bin/env python
""" Timeseries generator module """

from supremm.plugin import Plugin
from supremm.errors import ProcessingError

class CoverageTimeseries(Plugin):
    """ Generate the CPU usage as a timeseries data """

    name = property(lambda x: "coverage")
    mode = property(lambda x: "timeseries")
    # Do not care what metric is used, just that it exists
    requiredMetrics = property(lambda x: ["kernel.percpu.cpu.user"], ["kernel.all.cpu.user"], ["kernel.all.load"])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CoverageTimeseries, self).__init__(job)
        self._data = {}
        self._hostnames = {}
        self.THRESHOLD = 300

    def process(self, nodemeta, timestamp, data, description):

        if nodemeta.nodename not in self._hostnames:
            self._hostnames[nodemeta.nodename] = nodemeta.nodeindex
            self._data[nodemeta.nodeindex] = [[timestamp, timestamp]]

        lasttime = self._data[nodemeta.nodeindex][-1][1]
        if timestamp - lasttime > self.THRESHOLD:
            self._data[nodemeta.nodeindex].append([timestamp, timestamp])
        else:
            self._data[nodemeta.nodeindex][-1][1] = timestamp

        return True

    def results(self):

        print self._data
        return {"error": ProcessingError.JOB_TOO_SHORT}

