#!/usr/bin/env python

import operator
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from subprocess import Popen, PIPE

HOME = os.getenv("HOME")
USER = os.getenv("USER")

LOG_PATH = os.path.join(HOME, '.pbs_log')
PBS_PATH = os.path.join(HOME, 'pbs-output')
USER_LABEL = '*%s' % (USER,)


class JobStatusError(Exception):
    """Custom error thrown by jobstatus code"""


class Job(dict):
    """Simple class extending a dictionary with convenient functions for job details retrieval"""

    @property
    def cmd(self):
        return self.get('log_cmd', '').strip('"') or self.get('Run command') or '-'

    def cmd_trucated(self, length=32):
        cmd = self.cmd
        if len(cmd) > length:
            cmd = cmd[:length - 3] + '...'
        return cmd

    @property
    def job_id(self):
        return self['job_id']

    @property
    def exit_status(self):
        return self.get('Exit status', '-')

    @property
    def state(self):
        if self.exit_status not in ('-', '0'):
            return 'Failed'

        s = self.get('job_state', 'Completed' if 'Execution host' in self else '?')
        if 'queue' in self:
            s += ' (%s)' % self['queue']

        return s

    @property
    def start(self):
        if 'start_time' in self:
            return self['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        return ''

    @property
    def runtime(self):
        if 'Resource_List.walltime' in self:
            return '%s/%s' % (self.get('resources_used.walltime', '00:00:00'), self['Resource_List.walltime'])
        elif 'walltime' in self:
            return self['walltime']
        return ''

    @property
    def memory(self):
        if 'Resource_List.mem' in self:
            mem = float(self.get('resources_used.mem', '0kb')[:-2]) / (1024 * 1024)
            rmem = float(self.get('Resource_List.mem', '0mb')[:-2]) / 1024

            return '%.1f/%.1fG (%3d%%)' % (mem, rmem, mem / rmem)
        elif 'mem' in self:
            # Fixes a bug, where job is killed while writing to stdout, preventing it to add \n to the end of line,
            # so the job details are continued on the same line and not parsed
            return self['mem']
        return ''


class JobList(dict):
    """Modified dictionary that updates existing job data on item set"""

    def __setitem__(self, key, value):
        if key not in self:
            super(JobList, self).__setitem__(key, Job(value))
            self[key]['job_id'] = int(key.split('.')[0])
        else:
            self.__getitem__(key).update(value)

    def __iter__(self):
        return sorted(self.values(), key=lambda x: x.job_id, reverse=True).__iter__()


class TimeDelta:
    """Makes filtering job list by arbitrary constraints simpler"""

    def __init__(self, arg, newer=True):
        self.compare = operator.ge if newer else operator.le

        if re.match('^\d{4}-\d{2}-\d{2}$', arg):
            self.field = 'date'
            self.value = datetime.strptime(arg, '%Y-%m-%d')
        elif re.match('^\d+$', arg):
            self.field = 'job_id'
            self.value = int(arg)
        elif re.match('^\d+[hdw]$', arg):
            self.field = 'date'
            self.value = _parse_timearg(arg)
        else:
            raise JobStatusError("Unable to parse: %s" % arg)

    def filter(self, jobs):
        for job in jobs:
            if self.field == 'date' and self.compare(job['finished'], self.value):
                yield job
            elif self.field == 'job_id' and self.compare(job.job_id, self.value):
                yield job


def read_pbs_output(jobs=None):
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

    :return: Parsed jobs from ~/pbs-output/ folder
    :rtype: dict
    """
    jobs = jobs if jobs is not None else JobList()

    for out in os.listdir(PBS_PATH):
        # Parse only job files ending with:
        if not out.endswith('.bc.ccbr.utoronto.ca.OU'):
            continue

        job_id = out[:-3]  # remove .OU

        # Set ctime of the output file as execution end time
        out_data = {'finished': datetime.fromtimestamp(os.path.getctime(os.path.join(PBS_PATH, out)))}
        with open(os.path.join(PBS_PATH, out)) as fin:
            for line in fin:
                if line.startswith('==>'):  # Parse only useful details, ignore job output for now
                    param, val = line[4:].strip().split(':', 1)
                    param = param.strip()

                    if param == 'Resources used':
                        out_data.update([v.split('=') for v in val.strip().split(',')])
                    else:
                        out_data[param] = val.strip()

        jobs[job_id] = out_data

    return jobs


def read_pbs_log(jobs=None):
    """Parse .pbs_log file created by the new submitjob script for some extra info on running/finished jobs. Returns
    job_id -> (timestamp, command) pairs.

    :return: Parsed jobs from ~/.pbs_log file
    :rtype: Dict[String, Tuple[datetime, String]]
    """
    jobs = jobs if jobs is not None else JobList()

    if os.path.isfile(LOG_PATH):
        with open(LOG_PATH) as log:
            for l in log:
                timestamp, job_id, cmd = l.strip().split(None, 2)
                jobs[job_id] = {'start_time': datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S.%f]"), 'log_cmd': cmd}

    return jobs


def read_qstat_detailed(jobs=None):
    """Parse qstat -f output to get the most details about queued/running jobs of the user that executes this script.
    Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most useful ones are:
    resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...

    :return: Parsed jobs from qstat output
    :rtype: dict
    """
    proc = Popen('qstat -f', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    jobs = jobs if jobs is not None else JobList()

    job_re = re.compile("Job Id:[\s\S]*?(?=\nJob Id:|$)")  # Regex that parses each "Job Id" block
    job_param_re = re.compile('[ ]{4}[\s\S]*?(?=\n[ ]{4}|$)')  # Regex that parses each key=value pair from Job Id block

    for job in job_re.findall(qstat):
        job_id = job[8:job.index('\n')]
        job_data = dict([kv.strip().replace('\n\t', '').split(' = ') for kv in job_param_re.findall(job)])
        if job_data['euser'] == USER:  # Store only current user's jobs
            jobs[job_id] = job_data

    return jobs


def read_all():
    jobs = read_qstat_detailed()
    read_pbs_log(jobs)
    read_pbs_output(jobs)

    return jobs


def read_qstat():
    """Parses the brief qstat output for all users and makes 3 separate summaries: users, queues, total

    :return: Job summaries for users, queues and total
    :rtype: tuple[dict, dict, dict]
    """
    proc = Popen('qstat', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    queue_stats = defaultdict(lambda: defaultdict(int))
    total_stats = defaultdict(int)

    for line in qstat.split('\n')[2:]:  # skip first two rows of header
        if not line:
            continue

        job_id, name, user, time, status, queue = re.split('\s+', line.strip())

        user = USER_LABEL if user == USER else user
        user_stats[user][queue][status] += 1
        queue_stats[queue][status] += 1
        total_stats[status] += 1

    return user_stats, queue_stats, total_stats


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

    jobs = read_all()

    if args.failed_since:
        failed_check = TimeDelta(args.failed_since)

        for job in failed_check.filter(jobs):
            if job.state == 'Failed':
                print(job.cmd)
    else:
        columns = ' | '.join(['%-8s', '%-11s', '%-4s', '%-19s', '%-18s', '%-18s', '%-32s'])
        header = columns % ('Job ID', 'Status', 'Exit', 'Start Time', 'Elapsed/Total Time', 'Used Memory', 'Command')

        print(header)
        print('-' * len(header))

        i = 1
        for job in jobs:
            print(columns % (job.job_id, job.state, job.exit_status,
                             job.start, job.runtime, job.memory, job.cmd_trucated()))
            i += 1
            if i > args.limit_output:
                break


def archive(_args):
    print('ARCHIVE')
    for job in read_all():
        print(job.job_id, job.cmd)


def main():
    if len(sys.argv) == 1:  # Python 2 argparse hack... don't judge me
        print_all_jobs()
        return

    import argparse

    parser = argparse.ArgumentParser(
        description='Check job status. If no subcommand is specified it prints out a summary of all jobs.')

    command_parsers = parser.add_subparsers(title='Available subcommands',
                                            dest='command',
                                            description='For detailed subcommand help run: <subcommand> -h.', )

    details_parser = command_parsers.add_parser('details', help='Show details of my jobs.')
    details_parser.add_argument(
        '-f', '--failed-since',
        help='Print all failed commands after FAILED_SINCE. '
             'Must be either a date (YYYY-MM-DD) or Job ID (numeric part).')
    details_parser.add_argument('-l', '--limit-output', help='Limit output to this many lines (default: 50).',
                                default=50, type=int)
    details_parser.set_defaults(func=details)

    archive_parser = command_parsers.add_parser('archive', help='Archive finished jobs.')
    archive_parser.add_argument('-a', '--age', default='1m',
                                help='Archive finished jobs older than specified age. '
                                     'Allowed age format is <number><unit> (ie. 2w or 3m or 1d)'
                                     'Unit can be one of: d(days), w(weeks) or m(months)')
    archive_parser.set_defaults(func=archive)

    args = parser.parse_args()

    try:
        args.func(args)
    except JobStatusError as e:
        # Fail gracefully only for known errors
        parser.error(str(e))


if __name__ == '__main__':
    main()
