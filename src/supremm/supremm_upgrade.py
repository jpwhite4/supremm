#!/usr/bin/env python
""" supremm-upgrade script used to alter database or config files to latest
    schema versions """

import argparse
import signal
import subprocess
import sys

import pkg_resources

from supremm.xdmodstylesetupmenu import XDMoDStyleSetupMenu
from supremm.config import Config


def update_mysql_tables(display, opts):
    """ Interactive mysql script execution """

    display.newpage("MySQL Database setup")

    config = Config()

    migration = pkg_resources.resource_filename(__name__, "migrations/1.0-1.1/modw_supremm.sql")

    dbsettings = config.getsection("datawarehouse")
    host = dbsettings['host']
    port = dbsettings['port'] if 'port' in dbsettings else 3306

    myrootuser = display.prompt_string("DB Admin Username", "root")
    myrootpass = display.prompt_password("DB Admin Password")

    pflag = "-p{0}".format(myrootpass) if myrootpass != "" else ""
    shellcmd = "mysql -u {0} {1} -h {2} -P {3} < {4}".format(myrootuser,
                                                             pflag,
                                                             host,
                                                             port,
                                                             migration)
    try:
        if opts.debug:
            display.print_text(shellcmd)

        retval = subprocess.call(shellcmd, shell=True)
        if retval != 0:
            display.print_warning("""

An error occurred creating the tables. Please create the tables manually
following the documentation in the install guide.
""")
        else:
            display.print_text("Sucessfully created tables")
    except OSError as e:
        display.print_warning("""

An error:

\"{0}\"

occurred running the mysql command. Please create the tables manually
following the documentation in the install guide.
""".format(e.strerror))

    display.hitanykey("Press ENTER to continue.")


def signal_handler(sig, _):
    """ clean exit on an INT signal """
    if sig == signal.SIGINT:
        sys.exit(0)


def main():
    """ main entry point """
    parser = argparse.ArgumentParser(description='Upgrade the SUPReMM database and config files')
    parser.add_argument('-v', '--verbose', action='store_true', help='Output info level logging')
    parser.add_argument('-d', '--debug', action='store_true', help='Output debug level logging')
    parser.add_argument('-q', '--quiet', action='store_true', help='Output warning level logging')

    opts = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    with XDMoDStyleSetupMenu() as display:
        update_mysql_tables(display, opts)


if __name__ == "__main__":
    main()
