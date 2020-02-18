import os
import re

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
    with os.popen('stty size', 'r') as ttyin:
        _, WIDTH = map(int, ttyin.read().split())

# Some useful constants, python 2.6 compatible
UP_STATES = set(("job-exclusive", "job-sharing", "reserve", "free", "busy", "time-shared"))

RE_JOB = re.compile(r'(\d+/)?(\d+)[.].+')
RE_DC = re.compile(r'(.+)[.]o(\d+)')

# Adapted from: https://stackoverflow.com/a/14693789
ANSI_ESC = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
