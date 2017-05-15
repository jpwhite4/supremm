#!/usr/bin/env python
"""
Abstraction of the job accouting data
"""

from supremm.accounting import Accounting, ArchiveCache
import MySQLdb as mdb
from supremm import batch_acct
from supremm.Job import Job
from supremm.config import Config
from supremm.lariat import LariatManager

import datetime
import json
import sys

INGEST_VERSION = 0x000001
PROCESS_VERSION = 0x000001


class DbInsert(object):
    """
    Helper class that adds job accounting records to the database
    """

    def __init__(self, dbname, mydefaults):

        self.con = mdb.connect(db=dbname, read_default_file=mydefaults)
        self.buffered = 0
        self._hostnamecache = {}

        cur = self.con.cursor()
        cur.execute("SELECT resource_id, hostname FROM hosts")
        for host in cur:
            self.addtocache(host[0], host[1])

    def addtocache(self, resource_id, hostname):
        if resource_id not in self._hostnamecache:
            self._hostnamecache[resource_id] = {}
        self._hostnamecache[resource_id][hostname] = 1

    def checkcache(self, resource_id, hostname):
        if resource_id not in self._hostnamecache:
            return False
        if hostname not in self._hostnamecache[resource_id]:
            return False
        return True

    def insert(self, data, hostnames):
        """
        Insert a job record
        """
        cur = self.con.cursor()
        try:
            query = "INSERT INTO job (resource_id, local_job_id, start_time_ts, end_time_ts, record) VALUES(%s,%s,%s,%s,COMPRESS(%s)) ON DUPLICATE KEY UPDATE end_time_ts = VALUES(end_time_ts), record = VALUES(record)"
            cur.execute(query, data)
            self.con.commit()

            for host in hostnames:
                if False == self.checkcache(data[0], host):
                    cur.execute("INSERT IGNORE INTO hosts (resource_id, hostname) VALUES (%s, %s)", [data[0], host])
                    self.con.commit()
                    self.addtocache(data[0], host)

                try:
                    cur.execute("INSERT IGNORE INTO jobhosts (jobid, hostid) VALUES( (SELECT id FROM job WHERE resource_id = %s AND local_job_id = %s AND end_time_ts = %s), (SELECT id FROM hosts WHERE hostname = %s AND resource_id = %s) )",
                            [data[0], data[1], data[3], host, data[0]])
                except Exception as e:
                    print "INSERT INTO jobhosts (jobid, hostid) VALUES( (SELECT id FROM job WHERE resource_id = %s AND local_job_id = %s AND end_time_ts = %s), (SELECT id FROM hosts WHERE hostname = %s AND resource_id = %s) )" % (data[0], data[1], data[3], host, data[0])
                    raise e

            cur.execute("INSERT IGNORE INTO process (jobid, ingest_version) VALUES ( (SELECT id FROM job WHERE resource_id = %s AND local_job_id = %s AND end_time_ts = %s), %s)", [data[0], data[1], data[3], INGEST_VERSION])
        except mdb.IntegrityError as e:
            if e[0] != 1062:
                raise e
            # else:
                # Todo - check that the blobs match on duplicate records

        self.buffered += 1
        if self.buffered > 100:
            self.con.commit()
            self.buffered = 0

    def postinsert(self):
        """
        Must be called after insert.
        """
        self.con.commit()

    def getmostrecent(self, resource_id):
        """
        Get the end timestamp of the most recent job for the resource
        """
        query = "SELECT MAX(end_time_ts) FROM job WHERE resource_id = %s"
        data = (resource_id, )

        cur = self.con.cursor()
        cur.execute(query, data)
        return cur.fetchone()[0]


class DbArchiveCache(ArchiveCache):
    """
    Helper class that adds job accounting records to the database
    """

    def __init__(self, config):

        acctconf = config.getsection("accountdatabase")
        self.con = mdb.connect(db=acctconf['dbname'], read_default_file=acctconf['defaultsfile'])
        self.buffered = 0
        self._hostnamecache = {}

        cur = self.con.cursor()
        cur.execute("SELECT resource_id, hostname FROM hosts")
        for host in cur:
            self.addtocache(host[0], host[1])

    def addtocache(self, resource_id, hostname):
        if resource_id not in self._hostnamecache:
            self._hostnamecache[resource_id] = {}
        self._hostnamecache[resource_id][hostname] = 1

    def checkcache(self, resource_id, hostname):
        if resource_id not in self._hostnamecache:
            return False
        if hostname not in self._hostnamecache[resource_id]:
            return False
        return True

    def insert(self, resource_id, hostname, filename, start, end, jobid):
        """
        Insert a job record
        """
        cur = self.con.cursor()

        if False == self.checkcache(resource_id, hostname):
            cur.execute("INSERT IGNORE INTO hosts (resource_id, hostname) VALUES (%s, %s)", [resource_id, hostname])
            self.con.commit()
            self.addtocache(resource_id, hostname)

        filenamequery = """INSERT INTO `archive_paths` (`filename`) VALUES (%s) ON DUPLICATE KEY UPDATE id = id """

        cur.execute(filenamequery, [filename])
        if cur.lastrowid != 0:
            filenamequery = "%s"
            filenameparam = cur.lastrowid
        else:
            filenamequery = "(SELECT id FROM `archive_paths` WHERE `filename` = %s)"
            filenameparam = filename

        if jobid != None:
            query = """INSERT INTO `archives_joblevel` 
                            (archive_id, host_id, local_job_id_raw, start_time_ts, end_time_ts) 
                       VALUES (
                            {0},
                            (SELECT id FROM `hosts` WHERE resource_id = %s AND hostname = %s),
                            %s,
                            FLOOR(%s),
                            CEILING(%s)
                       )
                       ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
                    """.format(filenamequery)

            cur.execute(query, [filenameparam, resource_id, hostname, jobid, start, end])
        else:
            query = """INSERT INTO `archives_nodelevel`
                            (archive_id, host_id, start_time_ts, end_time_ts)
                       VALUES (
                            {0},
                            (SELECT id FROM `hosts` WHERE resource_id = %s AND hostname = %s),
                            FLOOR(%s),
                            CEILING(%s)
                       )
                       ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
                    """.format(filenamequery)

            cur.execute(query, [filenameparam, resource_id, hostname, start, end])

        self.buffered += 1
        if self.buffered > 100:
            self.con.commit()
            self.buffered = 0

    def postinsert(self):
        """
        Must be called after insert.
        """
        self.con.commit()


class DbLogger(object):
    """
    Helper class that marks job records as processed
    """

    def __init__(self, conf):
        dbconf = conf.getsection("accountdatabase")
        self.con = mdb.connect(db=dbconf['dbname'], read_default_file=dbconf['defaultsfile'])

    def dolog(self, acct, resource_id, version, ptime):
        """
        mark a job record as processed
        """

        query = """ UPDATE process p, job j
                    SET p.process_version = %s, p.process_timestamp = NOW(), p.process_time = %s
                    WHERE 
                        p.jobid = j.id 
                        AND j.resource_id = %s 
                        AND j.local_job_id = %s 
                        AND j.end_time_ts = %s """

        data = (version, ptime, resource_id, acct['id'], acct['end_time'])

        cur = self.con.cursor()
        cur.execute(query, data)
        self.con.commit()

    def logprocessed(self, acct, resource_id, ptime):
        """ mark a job as succesfully processed """
        self.dolog(acct, resource_id, PROCESS_VERSION, ptime)

    def logpending(self, acct, resource_id):
        """ mark a job as not processed """
        self.dolog(acct, resource_id, -1 * PROCESS_VERSION, None)


class DbAcct(Accounting):
    """
    Helper class to get job records from the store
    """

    def __init__(self, resource_id, conf, totalprocs=None, procid=None):
        super(DbAcct, self).__init__(resource_id, conf, totalprocs, procid)

        self._dblog = DbLogger(conf)
        dbconf = conf.getsection("accountdatabase")
        self.con = mdb.connect(db=dbconf['dbname'], read_default_file=dbconf['defaultsfile'])
        self.hostcon = mdb.connect(db=dbconf['dbname'], read_default_file=dbconf['defaultsfile'])
        self.process_version = PROCESS_VERSION
        self.totalprocs = totalprocs
        self.procid = procid

        self.hostquery = """SELECT
                           h.hostname, GROUP_CONCAT(ap.filename ORDER BY na.start_time_ts ASC SEPARATOR 0x1e)
                       FROM
                           `hosts` h,
                           `archive_paths` ap,
                           `archives_nodelevel` na,
                           `jobhosts` jh,
                           `job` j
                       WHERE
                           j.id = jh.jobid 
                           AND jh.jobid = %s 
                           AND jh.hostid = h.id
                           AND na.host_id = h.id
                           AND ((j.start_time_ts BETWEEN na.start_time_ts AND na.end_time_ts)
                           OR (j.end_time_ts BETWEEN na.start_time_ts AND na.end_time_ts)
                           OR (j.start_time_ts < na.start_time_ts
                           AND j.end_time_ts > na.end_time_ts))
                           AND ap.id = na.archive_id 
                       GROUP BY 1 """

    @staticmethod
    def recordtojob(record, hostlist, hostarchivemapping=None):
        """ convert an accounting record to a job class """

        if record['node_list'] == "None assigned":
            record['reqnodes'] = record['nodes']
            record['nodes'] = 0

        # The job primary key value is not used by this class, instead the
        # combination of acct[endtime] acct[localjobid] and resourceid is used
        # to ID thedb rows corresponding to the job

        j = Job(record['id'], record['id'], record)
        j.set_nodes(hostlist)

        if hostarchivemapping != None:
            j.set_rawarchives(hostarchivemapping)

        return j

    def getbylocaljobid(self, localjobid):
        """
        Search for a job based on the local job id
        """

        query = """SELECT
                        j.id,
                        UNCOMPRESS(j.record) 
                FROM 
                        `job` j
                WHERE 
                        j.resource_id = %s 
                        AND j.local_job_id LIKE %s """

        data = (self._resource_id, localjobid + '%')

        cur = self.con.cursor()
        cur.execute(query, data)

        for record in cur:
            jobid = record[0]
            r = json.loads(record[1])

            hostcur = self.hostcon.cursor()
            hostcur.execute(self.hostquery, (jobid, ))

            hostarchives = {}
            for h in hostcur:
                hostarchives[h[0]] = h[1].split(chr(0x1e))

            yield self.recordtojob(r, hostarchives.keys(), hostarchives)

    def getbytimerange(self, start, end, onlynew):
        """
        Search for jobs based on the time interval
        """

        query = """SELECT
                        j.id,
                        UNCOMPRESS(j.record) 
                FROM 
                        `job` j
                WHERE 
                        j.resource_id = %s 
                        AND j.end_time_ts BETWEEN unix_timestamp(%s) AND unix_timestamp(%s)
                """

        data = (self._resource_id, start, end)

        if onlynew != None and onlynew != False:
            query += " AND (p.process_version != %s OR p.process_version IS NULL)"
            data = data + (Accounting.PROCESS_VERSION, )

        if self.totalprocs != None and self.procid != None:
            query += " AND (CRC32(local_job_id) %% %s) = %s"
            data = data + (self.totalprocs, self.procid)

        query += " ORDER BY j.end_time_ts ASC"

        cur = self.con.cursor()
        cur.execute(query, data)

        for record in cur:
            jobid = record[0]
            r = json.loads(record[1])

            hostcur = self.hostcon.cursor()
            hostcur.execute(self.hostquery, (jobid, ))

            hostarchives = {}
            for h in hostcur:
                hostarchives[h[0]] = h[1].split(chr(0x1e))

            yield self.recordtojob(r, hostarchives.keys(), hostarchives)

    def get(self, start_time=None, end_time=None):
        """ 
        read all unprocessed jobs between start_time and end_time (or all time if start/end not specified)
        """

        query = """SELECT 
                        j.id,
                        UNCOMPRESS(j.record) 
                FROM 
                        `job` j, 
                        `process` p
                WHERE 
                        j.id = p.jobid 
                        AND j.resource_id = %s 
                        AND p.process_version != %s """

        data = (self._resource_id, PROCESS_VERSION)
        if start_time != None:
            query += " AND end_time_ts >= %s "
            data = data + (start_time, )
        if end_time != None:
            query += " AND end_time_ts < %s "
            data = data + (end_time, )
        if self.totalprocs != None and self.procid != None:
            query += " AND (CRC32(local_job_id) %% %s) = %s"
            data = data + (self.totalprocs, self.procid)
        query += " ORDER BY end_time_ts ASC"

        cur = self.con.cursor()
        cur.execute(query, data)

        for record in cur:
            jobid = record[0]
            r = json.loads(record[1])

            hostcur = self.hostcon.cursor()
            hostcur.execute(self.hostquery, (jobid, ))

            hostarchives = {}
            for h in hostcur:
                hostarchives[h[0]] = h[1].split(chr(0x1e))

            yield self.recordtojob(r, hostarchives.keys(), hostarchives)

    def markasdone(self, job, success, elapsedtime, error):
        if success:
            self._dblog.logprocessed(job.acct, self._resource_id, elapsedtime)
        else:
            # elapsed time ignored for pending jobs
            self._dblog.logpending(job.acct, self._resource_id)


def ingestall(config):
    """ 
    Run account data ingest for all records
    """
    ingest(config, 9223372036854775807L, 0)


def ingest(config, end_time, start_time=None):
    """ 
    Run account data ingest for all records between start_time and end_time
    If start_time is not specified then the start time is based on the
    most recent record last ingested
    """

    dbconf = config.getsection("accountdatabase")
    dbif = DbInsert(dbconf["dbname"], dbconf["defaultsfile"])

    for resourcename, resource in config.resourceconfigs():

        if resource['batch_system'] == "XDMoD":
            continue

        if start_time == None:
            start_time = dbif.getmostrecent(resource['resource_id'])
            if start_time == None:
                start_time = 0
            else:
                start_time = start_time - (7 * 24 * 3600)

        acctreader = batch_acct.factory(resource['batch_system'], resource['acct_path'], resource['host_name_ext'])

        if 'lariat_path' in resource:
            lariat = LariatManager(resource['lariat_path'])
        else:
            lariat = None

        for acct in acctreader.reader(start_time, end_time):

            if lariat != None:
                acct['lariat'] = lariat.find(acct['id'], acct['start_time'], acct['end_time'])

            record = []
            record.append(resource['resource_id'])
            record.append(acct['id'])
            record.append(acct['start_time'])
            record.append(acct['end_time'])
            record.append(json.dumps(acct))

            if 'host_list_dir' in resource:
                hostlist = acctreader.get_host_list_path(acct, resource['host_list_dir'])
                hostnames = []
                if hostlist != None:
                    with open(hostlist, "r") as fp:
                        if resource['hostname_mode'] == "fqdn" and resource['host_name_ext'] != "":
                            hostnames = [x.strip() + "." + resource['host_name_ext'] for x in fp]
                        else:
                            hostnames = [x.strip() for x in fp]
            else:
                hostnames = acct['host_list']

            dbif.insert(record, hostnames)

        dbif.postinsert()


def runingest():
    if len(sys.argv) > 1:
        config = Config(sys.argv[1])
    else:
        config = Config()

    if len(sys.argv) > 2:
        end_time = sys.argv[2]
    else:
        end_time = 9223372036854775807L

    #ingest(config, end_time)
    ingestall(config)

if __name__ == "__main__":
    runingest()
