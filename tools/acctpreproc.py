#!/usr/bin/env python
import os
import json
import sys
import datetime
import re

class AcctFileSubset(object):
    def __init__(self, config):
        self.config = config
        self.datere = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.")

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

            if self.config['mindate'] != None:
                datematch = self.datere.match(filename)
                if datematch:
                    filedate = datetime.datetime(year=int(datematch.group(1)), month=int(datematch.group(2)), day=int(datematch.group(3)))
                    if filedate < self.config['mindate']:
                        continue

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

        if '-a' in sys.argv:
            rset['mindate'] = None
        else:
            rset['mindate'] = datetime.datetime.now() - datetime.timedelta(days=3)
            
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
