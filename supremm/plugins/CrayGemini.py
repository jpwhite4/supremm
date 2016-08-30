#!/usr/bin/env python

from supremm.plugin import Plugin
from supremm.statistics import calculate_stats
import numpy

class CrayGemini(Plugin):
    """ Metrics from the Cray Gemini interconnect """

    name = property(lambda x: "gemini")
    mode = property(lambda x: "firstlast")
    requiredMetrics = property(lambda x: [
        "gemini.totaloutput_optA",
        "gemini.totalinput",
        "gemini.fmaout",
        "gemini.bteout_optA",
        "gemini.bteout_optB",
        "gemini.totaloutput_optB"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

    def __init__(self, job):
        super(CrayGemini, self).__init__(job)
        self._first = {}
        self._data = {}
        self._error = None

    def process(self, nodemeta, timestamp, data, description):

        if len(data[0]) == 0:
            return False

        if nodemeta.nodeindex not in self._first:
            self._first[nodemeta.nodeindex] = numpy.array(data)
            return True

        hostdata = numpy.array(data) - self._first[nodemeta.nodeindex]

        for idx, metricname in enumerate(self.requiredMetrics):
            if metricname not in self._data:
                self._data[metricname] = []
            self._data[metricname].append(hostdata[idx, 0])

    def results(self):

        if self._error != None:
            return {"error": self._error}

        if len(self._data) == 0:
            return {"error": ProcessingError.INSUFFICIENT_DATA}

        output = {}

        for metricname, metric in self._data.iteritems():
            prettyname = "-".join(metricname.split(".")[1:])
            output[prettyname] = calculate_stats(metric)

        return output

