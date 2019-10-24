#!/usr/bin/env python
import hashlib
import operator
import os
import random
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from subprocess import Popen, PIPE
from tarfile import TarFile

HOME = os.getenv("HOME")
USER = os.getenv("USER")
WIDTH = os.getenv("COLUMNS")

if not WIDTH:
    with os.popen('stty size', 'r') as ttyin:
        try:
            _, WIDTH = map(int, ttyin.read().split())
        except ValueError:
            WIDTH = 120

LOG_PATH = os.path.join(HOME, '.pbs_log')
PBS_PATH = os.path.join(HOME, 'pbs-output')
PBS_ARCHIVE_PATH = os.path.join(PBS_PATH, 'archive')
USER_LABEL = '*%s' % (USER,)

RE_DC = re.compile(r'(.+)[.]o(\d+)')
# Adapted from: https://stackoverflow.com/a/14693789
ANSI_ESC = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


class JobStatusError(Exception):
    """Custom error thrown by jobstatus code"""


class TimeDeltaError(Exception):
    """Custom error thrown when parsing time delta"""


def confirm_delete(question, confirmation_string):
    """Ask a question via raw_input() with a string the user must repeat to confirm.
    Code adapted from: https://stackoverflow.com/a/3041990
    """
    input_func = input
    if '__builtin__' in sys.modules:  # if we're using python2, fallback to raw_input
        input_func = sys.modules['__builtin__'].raw_input

    prompt = "\nConfirm by typing in the number of jobs to be deleted: "

    while True:
        sys.stdout.write(question + prompt)
        choice = input_func().lower()
        return choice == str(confirmation_string)


def _parse_timearg(arg, since=datetime.now()):
    """Parse a human readable timedelta option: 5h,3w,2d,... and subtracts it from the date

    :param arg: timedelta string to parse
    :param since: reference datetime, or now() by default
    :type arg: string
    :type since: datetime
    :return: Adjusted datetime
    :rtype: datetime
    """
    amount = int(arg[:-1])
    period = arg[-1]

    return since - timedelta(
        **{{'h': 'hours', 'd': 'days', 'w': 'weeks'}[period]: amount}
    )


def generic_to_gb(val):
    """Convert any random unit to GB

    :param val: size in any unit (including two letter unit)
    :type val: str
    :return: size in GB
    :rtype: float
    """
    unit = val[-2:].lower()
    val = int(val[:-2])
    return val / {'kb': 1048576., 'mb': 1024., 'gb': 1.}[unit]


def cache_cmd(cmd, max_seconds=60, ignore_cache=False):
    """ Run and cache the command for 1min

    :param cmd: Command to execute
    :param max_seconds: How many seconds should the output be cached
    :param ignore_cache: Ignore cached output, re-run the command
    :type cmd: str
    :type max_seconds: int
    :type ignore_cache: bool
    :return: cmd output
    :rtype: str
    """

    hsh = hashlib.sha1(cmd.encode()).hexdigest()
    cached_file = os.path.join('/tmp', '{user}-{hash}'.format(user=USER, hash=hsh))
    now = datetime.now()

    if not ignore_cache and os.path.exists(cached_file):
        age = now - datetime.fromtimestamp(os.path.getmtime(cached_file))
        if age.total_seconds() < max_seconds:
            with open(cached_file) as cached_in:
                return cached_in.read()

    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    ret, err = proc.communicate()
    if err:
        raise Exception("Can't run %s: %s" % (cmd, err))

    ret = ANSI_ESC.sub('', ret)

    with open(cached_file, 'w') as cached_out:
        cached_out.write(ret)

    return ret


def read_xml(cmd, *args, **kwargs):
    """ Execute cmd and parse the output XML, any additional args are passed to cache_cmd

    :param cmd: Command to run
    :type cmd: str
    :return: List of children elements in xml root
    :rtype: list[Et.Element]
    """
    import xml.etree.cElementTree as Et

    qstat = cache_cmd(cmd, *args, **kwargs)

    if not qstat:
        return []

    return Et.fromstring(qstat)  # trim any color escape sequences returned by our command


def truncate_str(s, length=32):
    """ Shorten the string to length and add 3 dots

    :param s: String to truncate
    :param length: Truncate threshold
    :type s: str
    :type length: int
    :return: Truncated string
    """
    if len(s) > length:
        s = s[:length - 3] + '...'
    return s


class Job:
    job_id = None
    mem = 2.  # 2GB default memory
    node = None
    pbs_log = None
    pbs_output = None
    finished = None
    start_time = None
    qstat = False

    # Variables to print
    name = ''
    state = '?'
    exit_status = '-'
    start = ''
    runtime = ''
    memory = ''
    cmd = ''

    def __init__(self):
        pass

    def parse_qstat(self, job):
        """ Object representing one Job as parsed from qstat output

        :param job: Job details from qstat
        :type job: dict
        """
        self.job_id = int(job['Job_Id'].split('.')[0])
        if 'Resource_List.mem' in job:
            self.mem = generic_to_gb(job['Resource_List.mem'])

        if job.get('exec_host'):
            self.node = job['exec_host'].split('.')[0]

        self.state = job.get('job_state', self.state)
        if 'queue' in job:
            self.state += ' (%s)' % job['queue']

        if 'Resource_List.walltime' in job:
            self.runtime = '%s/%s' % (job.get('resources_used.walltime', '00:00:00'), job['Resource_List.walltime'])

        self.name = job.get('Job_Name', self.name)

        used_mem = generic_to_gb(job.get('resources_used.mem', '0gb'))
        self.memory = '%.1f/%.1fG (%3d%%)' % (used_mem, self.mem, used_mem / self.mem * 100)
        self.qstat = True

    def parse_pbs_log(self, job_id, start_time, cmd, log_line):
        """ Parse this job from $HOME/.pbs_log

        :param job_id: Job ID
        :param start_time: Time when the job was submitted
        :param cmd: Submitted command
        :param log_line: Entire line from .pbs_log
        :type job_id: str
        :type start_time: datetime
        :type cmd: str
        :type log_line: str
        """
        self.job_id = int(job_id)
        self.start_time = start_time
        self.start = start_time.strftime('%Y-%m-%d %H:%M:%S')
        self.cmd = cmd[1:-1]
        self.pbs_log = log_line

    def parse_pbs_output(self, output):
        """ Parse this job from $HOME/pbs-output

        :param output: Parsed output file
        :type output: dict
        """
        self.job_id = int(output['job_id'])
        self.exit_status = output.get('Exit status', self.exit_status)
        self.finished = output.get('finished')

        if self.exit_status not in ('-', '0'):
            self.state = 'Failed'
        else:
            self.state = 'Completed'

        if not self.cmd:
            self.cmd = output.get('Run command', '-')

        self.runtime = output.get('walltime', self.runtime)

        self.memory = output.get('mem', self.memory)
        self.pbs_output = output['pbs_output']


class Jobs:
    jobs = defaultdict(Job)

    def __init__(self, cache_cmds):
        self.cache_cmds = cache_cmds

        self.read_qstatx()
        self.read_pbs_log()
        self.read_pbs_output()

    @staticmethod
    def collect(cache_cmds=True):
        return Jobs(cache_cmds).jobs_list

    @property
    def jobs_list(self):
        return sorted(self.jobs.values(), key=lambda x: x.job_id, reverse=True)

    def read_qstatx(self):
        """Parse qstat -x output to get the most details about queued/running jobs of the user that executes this
        script. Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most useful
        ones are: resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...
        This is the XML parsing version. Should be a bit safer than parsing regular output with RE.
        """
        for jobele in read_xml('/usr/bin/qstat -x', ignore_cache=not self.cache_cmds):
            job = dict([(attr.tag, attr.text) for attr in jobele])
            job['Job_Id'] = job['Job_Id'].split('.')[0]

            if job.get('euser') == USER:
                for ts in ['qtime', 'mtime', 'ctime', 'etime']:
                    if ts in job:
                        job[ts] = datetime.fromtimestamp(int(job[ts]))

                if 'Resource_List' in job:
                    job.pop('Resource_List')
                    for rl in jobele.find('Resource_List'):
                        job['Resource_List.%s' % rl.tag] = rl.text

                if 'resources_used' in job:
                    job.pop('resources_used')
                    for rl in jobele.find('resources_used'):
                        job['resources_used.%s' % rl.tag] = rl.text

                self.jobs[job['Job_Id']].parse_qstat(job)

    def read_pbs_log(self):
        """Parse .pbs_log file created by the new submitjob script for some extra info on running/finished jobs. Returns
        job_id -> (timestamp, command) pairs.
        """
        if os.path.isfile(LOG_PATH):
            with open(LOG_PATH) as log:
                for l in log:
                    timestamp, job_id, cmd = l.strip().split(None, 2)
                    job_id = job_id.split('.')[0]
                    try:
                        start_time = datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S.%f]")
                    except ValueError:
                        start_time = datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S]")

                    self.jobs[job_id].parse_pbs_log(job_id, start_time, cmd, l)

    def read_pbs_output(self):
        """Parse all job output files in ~/pbs-output/ folder and return the details as a job_id -> job_details pairs.
        Known job_details keys are:
        1. "Run command"
        2. "Execution host"
        3. "Exit status"
        "Resources used" is parsed further into:
        4.1 "cput"
        4.2 "walltime"
        4.3 "mem"
        4.4 "vmem"
        """
        for out in os.listdir(PBS_PATH):
            name = ''

            # Parse only job files ending with:
            if out.endswith('.bc.ccbr.utoronto.ca.OU'):  # banting cluster
                job_id = out.split('.')[0]
            elif RE_DC.match(out):  # DC cluster... ie: python.o70
                matcher = RE_DC.match(out)
                name = matcher.group(1)
                job_id = matcher.group(2)
            else:
                continue

            # Set ctime of the output file as execution end time
            out_data = {
                'job_id': job_id,
                'finished': datetime.fromtimestamp(os.path.getctime(os.path.join(PBS_PATH, out))),
                'pbs_output': os.path.join(PBS_PATH, out),
                'name': name}

            with open(os.path.join(PBS_PATH, out)) as fin:
                for line in fin:
                    if line.startswith('==>'):  # Parse only useful details, ignore job output for now
                        param, val = line[4:].strip().split(':', 1)
                        param = param.strip()

                        if param == 'Resources used':
                            out_data.update([v.split('=') for v in val.strip().split(',')])
                        else:
                            out_data[param] = val.strip()

            self.jobs[job_id].parse_pbs_output(out_data)


class TimeDelta:
    """Makes filtering job list by arbitrary constraints simpler"""

    def __init__(self, arg, newer=True):
        self.compare = operator.ge if newer else operator.le

        if re.match(r'^\d{4}-\d{2}-\d{2}$', arg):
            self.field = 'date'
            self.value = datetime.strptime(arg, '%Y-%m-%d')
        elif re.match(r'^\d+[a-cn-u.]*-*\d*[a-cn-u.]*$', arg):
            self.field = 'job_id'
            if '-' in arg:
                self.value_min = int(arg.split('-')[0].split('.')[0])
                self.value_max = int(arg.split('-')[1].split('.')[0])
            else:
                self.value_min = int(arg.split('.')[0])
        elif re.match(r'^\d+[hdw]$', arg):
            self.field = 'date'
            self.value = _parse_timearg(arg)
        else:
            raise TimeDeltaError("Unable to parse: %s" % arg)

    def filter(self, jobs):
        for job in jobs:
            if self.field == 'date':
                if job.finished:
                    if self.compare(job.finished, self.value):
                        yield job
                elif not job.qstat and job.start_time:
                    if self.compare(job.start_time, self.value):
                        yield job
            elif self.field == 'job_id':
                if self.compare(job.job_id, self.value_min):
                    if hasattr(self, 'value_max'):
                        if not operator.le(job.job_id, self.value_max):
                            continue
                    yield job


def read_qstat():
    """Parses the brief qstat output for all users and makes 3 separate summaries: users, queues, total

    :return: Job summaries for users, queues and total
    :rtype: tuple[dict, dict, dict]
    """
    qstat = cache_cmd('/usr/bin/qstat')

    user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    queue_stats = defaultdict(lambda: defaultdict(int))
    total_stats = defaultdict(int)

    for line in qstat.split('\n')[2:]:  # skip first two rows of header
        if not line:
            continue

        job_id, name, user, time, status, queue = line.strip().split()

        user = USER_LABEL if user == USER else user
        user_stats[user][queue][status] += 1
        queue_stats[queue][status] += 1
        total_stats[status] += 1

    return user_stats, queue_stats, total_stats


def print_all_jobs():
    """Print a short summary of running/queued jobs. Identical to the old jobstatus script."""
    user_stats, queue_stats, total_stats = read_qstat()

    print("=========================================================")
    print("%-15s %-10s %-10s %-10s %-10s" % ('User', 'Queue', 'Running', 'Queued', 'Exiting'))
    print("---------------------------------------------------------")

    statuses = ('R', 'Q', 'E')

    for user in sorted(user_stats):
        for queue in sorted(user_stats[user]):
            row = tuple([user, queue] + [user_stats[user][queue].get(s, 0) for s in statuses])
            print("%-15s %-10s %-10s %-10s %-10s" % row)

    print("---------------------------------------------------------")

    for queue in sorted(queue_stats):
        row = tuple(['', queue] + [queue_stats[queue].get(s, 0) for s in statuses])
        print("%-15s %-10s %-10s %-10s %-10s" % row)

    print("                -----------------------------------------")

    row = tuple(['', 'totals'] + [total_stats.get(s, 0) for s in statuses])
    print("%-15s %-10s %-10s %-10s %-10s" % row)


def details(args):
    """Print job details for current user. Output format can be fine-tuned with args argument.

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    # Don't cache commands if we're deleting jobs, we need fresh status
    jobs = Jobs.collect(cache_cmds=not args.delete)

    filtering = True in (args.print_running, args.print_queued, args.print_completed, args.print_failed)

    # We're about to delete some jobs, make sure to sanitize other arguments to make sense with delete action
    if args.delete:
        # Override to table view when deleting jobs, you want to see what'll get killed
        if args.output != 'table':
            sys.stderr.write('Warning: Output format forced from "%s" to "table" for delete action.\n' % args.output)
            args.output = 'table'

        # We can delete only running and queued, make sure they're both included if either is not explicitly enabled
        if not args.print_queued and not args.print_running:
            args.print_queued = True
            args.print_running = True

        filtering = True

        # Get rid of completed and failed if they were explicitly defined
        if args.print_completed or args.print_failed:
            sys.stderr.write('Warning: Ignore completed and failed jobs for delete action.\n')
            args.print_completed = False
            args.print_failed = False

        # Limiting by number of jobs makes no sense for deleting, get rid of it
        if args.limit_output and args.limit_output.isdigit() and int(args.limit_output) < 10000:
            sys.stderr.write('Warning: Filtering by number of jobs (%s) ignored.\n' % args.limit_output)
            args.limit_output = False

    if filtering:
        if not args.print_running:
            jobs = [job for job in jobs if not job.state.startswith('R')]
        if not args.print_queued:
            jobs = [job for job in jobs if not job.state.startswith('Q')]
        if not args.print_completed:
            jobs = [job for job in jobs if not job.state.startswith('C')]
        if not args.print_failed:
            jobs = [job for job in jobs if not (job.state.startswith('F') or job.state == '?')]

    if args.limit_output:
        if args.limit_output.isdigit():
            if int(args.limit_output) < 10000:
                jobs = jobs[:int(args.limit_output)]
            else:
                limit_check = TimeDelta(args.limit_output)
                jobs = limit_check.filter(jobs)
        else:
            try:  # filter by time
                limit_check = TimeDelta(args.limit_output)
                jobs = limit_check.filter(jobs)
            except TimeDeltaError:  # try filtering by name
                jobs = [job for job in jobs if job.name == args.limit_output]

    if args.output == 'jobid':
        jobids = [str(job.job_id) for job in jobs]
        print(' '.join(jobids))
    elif args.output == 'cmd':
        for job in jobs:
            print(job.cmd)
    else:
        columns = ['%-8s', '%-20s', '%-11s', '%-4s', '%-19s', '%-18s', '%-18s', '%-32s']
        headers = ('Job ID', 'Name', 'Status', 'Exit', 'Start Time', 'Elapsed/Total Time', 'Used Memory', 'Command')

        out_len = ' | '.join(columns[:-1]) % headers[:-1]

        free_space = max(32, WIDTH - 3 - len(out_len))

        columns[-1] = '%%-%ds' % free_space

        columns = ' | '.join(columns)
        header = columns % headers
        print(header)
        print('-' * len(header))

        for job in jobs:
            print(columns % (job.job_id, truncate_str(job.name, 20), job.state, job.exit_status,
                             job.start, job.runtime, job.memory, truncate_str(job.cmd, free_space)))

    if args.delete:
        if not len(jobs):
            print("\n\nNo jobs to delete.")
            return

        print("\n\nDANGER ZONE!")
        if confirm_delete('Are you sure you want to delete %s jobs listed above?' % len(jobs), len(jobs)):
            ids = [str(j.job_id) for j in jobs]
            proc = Popen('qdel %s' % ' '.join(ids), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                         universal_newlines=True)
            qdel, err = proc.communicate()
            if err:
                raise Exception("Can't run qdel: %s" % err)
            print("Deleted %d jobs." % len(ids))
        else:
            print("Wrong answer, not deleting anything.")


def archive(args):
    """Archive old finished jobs, save them in a gzipped file

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    timefilter = TimeDelta(args.age, newer=False)

    jobs = Jobs.collect()
    jobs_to_archive = []

    for job in timefilter.filter(jobs):
        if job.qstat:
            # Do not delete running jobs!
            continue

        if job.pbs_output or job.pbs_log:
            jobs_to_archive.append(job)

    if not jobs_to_archive:
        # Bail, nothing to do here
        return

    # Delete files only when we know they are all safely zipped
    delete_list = []
    archived_job_ids = set()

    if not os.path.exists(PBS_ARCHIVE_PATH):
        os.mkdir(PBS_ARCHIVE_PATH)

    tar_file = '%s_%032x.tar.gz' % (datetime.now().strftime('%Y-%m-%d'), random.getrandbits(128))
    tar_path = os.path.join(PBS_ARCHIVE_PATH, tar_file)

    with TarFile.open(tar_path, 'w:gz') as tar:
        for job in jobs_to_archive:
            if job.pbs_output:
                tar.add(job.pbs_output, arcname=job.pbs_output.replace(HOME, '').lstrip('/'))
                delete_list.append(job.pbs_output)

            if job.pbs_log:
                archived_job_ids.add(job.job_id)

            print('Archived job %s' % job.job_id)

    with open(LOG_PATH + '_bkp', 'w') as log:
        for job in jobs[::-1]:
            if job.pbs_log and job.job_id not in archived_job_ids:
                log.write(job.pbs_log)

    for f in delete_list:
        os.remove(f)

    os.rename(LOG_PATH + '_bkp', LOG_PATH)


def main():
    if len(sys.argv) == 1:  # Python 2 argparse hack... don't judge me
        print_all_jobs()
        return

    # noinspection PyCompatibility
    import argparse
    timedelta_help = 'Must be either a date (YYYY-MM-DD), Job ID (numeric part) or a time delta (2w, 3h or 1d). ' \
                     'Time delta unit can be one of: h(hours), d(days) or w(weeks)'

    parser = argparse.ArgumentParser(
        description='Check job status. If no subcommand is specified it prints out a summary of all jobs.')

    command_parsers = parser.add_subparsers(title='Available subcommands',
                                            dest='command',
                                            description='For detailed subcommand help run: <subcommand> -h.')

    details_parser = command_parsers.add_parser('details', help='Show details of my jobs.')
    details_parser.add_argument('-r', '--print-running', action='store_true', help='Print running jobs.')
    details_parser.add_argument('-q', '--print-queued', action='store_true', help='Print queued jobs.')
    details_parser.add_argument('-c', '--print-completed', action='store_true', help='Print completed jobs.')
    details_parser.add_argument('-f', '--print-failed', action='store_true', help='Print failed jobs.')
    details_parser.add_argument('-d', '--delete', action='store_true', help='Delete listed jobs.')
    details_parser.add_argument('-l', '--limit-output', default='50',
                                help='Limit output to either: number of lines, Job ID, time delta or name. '
                                     'The default is 50 lines. '
                                     'Job ID can be in a form of range (i.e. 28327149-28327165). '
                                     'Time delta unit can be one of: h(hours), d(days) or w(weeks).')
    details_parser.add_argument('-o', '--output', default='table',
                                help='Choose how to display output: table, jobid or cmd (default: table). '
                                     'TABLE dislays all available information about the job. '
                                     'JOBID displays space-separated job IDs which is useful for deleting jobs. '
                                     'CMD displays the commands which is useful for resubmitting jobs.')
    details_parser.set_defaults(func=details)

    archive_parser = command_parsers.add_parser('archive', help='Archive finished jobs.')
    archive_parser.add_argument('age', default='1w', nargs='?',
                                help='Archive finished jobs older than AGE (default: 1 week). ' + timedelta_help)
    archive_parser.set_defaults(func=archive)

    args = parser.parse_args()

    try:
        args.func(args)
    except JobStatusError as e:
        # Fail gracefully only for known errors
        parser.error(str(e))


if __name__ == '__main__':
    main()
