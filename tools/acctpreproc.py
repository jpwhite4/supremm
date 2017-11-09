#!/usr/bin/env python
import os
import json
import sys

class AcctFileSubset(object):
    def __init__(self, config):
        self.config = config

    def subsetfile(self, filename):
        with open(filename, "r") as filep:
            outfilename = os.path.join(self.config['outdir'], self.config['name'], os.path.basename(filename))
            with open(outfilename, "w") as outfile:
                for line in filep:
                    tokens = line.split(self.config['delimiter'])
                    if len(tokens) < self.config['field']:
                        continue
                    if tokens[self.config['field']] in self.config['include']:
                        outfile.write(line)

    def process(self):
        for filename in os.listdir(self.config['datasource']):
            fullpath = os.path.join(self.config['datasource'], filename)
            if os.path.isfile(fullpath):
                self.subsetfile(fullpath)

def getconfig(configfile):

    with open(configfile) as confp:
        config = json.load(confp)

    for resource, settings in config['resources'].iteritems():
        rset = config['defaults'].copy()
        rset.update(settings)
        rset['name'] = resource

        yield rset


def main():

    confpath = 'config.json'
    if sys.argv[1] == '--config' or sys.argv[1] == '-c':
        confpath = sys.argv[2]

    for config in getconfig(confpath):
        acct = AcctFileSubset(config)
        acct.process()

if __name__ == "__main__":
    main()
