import os
import re
import subprocess

USER = os.getenv("USER")
HOME = os.getenv("HOME")
PATH = os.getenv('PATH')
CWD = os.getcwd()

USER_LABEL = '*%s' % (USER,)

LOG_PATH = os.path.join(HOME, '.pbs_log')
PBS_OUTPUT = os.path.join(HOME, 'pbs-output')
PBS_ARCHIVE_PATH = os.path.join(PBS_OUTPUT, 'archive')

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
JOB_TEMPLATE = os.path.join(SCRIPT_PATH, 'qsub_job.template')

# Get terminal width for nicer printing
WIDTH = os.getenv("COLUMNS")

if not WIDTH:
    try:
        stty_size = subprocess.check_output(['stty', 'size'], stderr=subprocess.STDOUT)
        _, WIDTH = map(int, stty_size.decode().split())
    except subprocess.CalledProcessError:
        WIDTH = 120  # Default width, ie: called remotely via pssh or similar

# Some useful constants, python 2.6 compatible
UP_STATES = set(("job-exclusive", "job-sharing", "reserve", "free", "busy", "time-shared"))

RE_JOB = re.compile(r'(\d+/)?(\d+)[.].+')
RE_DC = re.compile(r'(.+)[.]o(\d+)')

# Adapted from: https://stackoverflow.com/a/14693789
ANSI_ESC = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')

CLUSTER_NAME = 'unknown'
if os.path.exists('/etc/torque/server_name'):
    CLUSTER_NAME = open('/etc/torque/server_name', 'r').read().strip()
elif os.path.exists('/etc/pbs.conf'):
    for line in open('/etc/pbs.conf', 'r').readlines():
        if line.startswith('PBS_SERVER'):
            CLUSTER_NAME = line.split('=', maxsplit=1)[-1].strip()
