#!/usr/bin/env python
import operator
import os
import random
import re
import sys
from collections import defaultdict
from datetime import datetime
from subprocess import Popen, PIPE
from tarfile import TarFile

from cluster.common import Cluster
from cluster.config import USER, LOG_PATH, USER_LABEL, PBS_ARCHIVE_PATH, HOME
from cluster.tools import confirm_delete, parse_timearg, truncate_str, cache_cmd, print_table


class TimeDeltaError(Exception):
    """Custom error thrown when parsing time delta"""


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
        elif ',' in arg:
            self.compare = operator.contains
            self.field = 'job_id_list'
            self.value = [int(j) for j in arg.split(',')]
        elif re.match(r'^\d+[hdw]$', arg):
            self.field = 'date'
            self.value = parse_timearg(arg)
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
            if self.field == 'job_id_list':
                if self.compare(self.value, job.job_id):
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
    cluster = Cluster(jobs_qstat=True, jobs_log=True, jobs_pbs=True, cached=not args.delete)
    jobs = cluster.jobs_list()

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
            args.limit_output = None

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
        data = []
        for job in jobs:
            data.append(
                [str(job.job_id), truncate_str(job.name, 20), job.state, job.exit_status, job.start, job.runtime,
                 job.memory, job.cmd])

        print_table(
            ['Job ID', 'Name', 'Status', 'Exit', 'Start Time', 'Elapsed/Total Time', 'Used Memory', 'Command'],
            data
        )

    if args.delete:
        jobs = list(jobs)
        if not len(jobs):
            print("\n\nNo jobs to delete.")
            return

        print("\n\nDANGER ZONE!")
        if confirm_delete('Are you sure you want to delete %s jobs listed above?' % len(jobs), str(len(jobs))):
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

    cluster = Cluster(jobs_qstat=True, jobs_log=True, jobs_pbs=True)
    jobs = cluster.jobs_list()

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


def subcommand_header(name):
    header = r"""
/-------------{pad}-\
| Subcommand: {name} |
\-------------{pad}-/""".format(pad='-' * len(name), name=name)
    return header


def main():
    if len(sys.argv) == 1:  # Python 2 argparse hack... don't judge me
        print_all_jobs()
        return

    # noinspection PyCompatibility
    import argparse
    timedelta_help = 'Must be either a date (YYYY-MM-DD), Job ID (numeric part) or a time delta (2w, 3h or 1d). ' \
                     'Time delta unit can be one of: h(hours), d(days) or w(weeks)'

    parser = argparse.ArgumentParser(
        description='Check job status. If no subcommand is specified it prints out a summary of all jobs.',
        add_help=False
    )
    parser.add_argument('-h', '--help', action='store_true', dest='help', default=False)

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
                                     'Job ID can be in a form of range (i.e. 28327149-28327165) or a comma separated '
                                     'list of ids. Time delta unit can be one of: h(hours), d(days) or w(weeks).')
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

    if len(sys.argv) == 2 and sys.argv[1] in ('-h', '--help'):
        # Another py2 hack, for a global help
        parser.print_help()

        for subp in (details_parser, archive_parser):
            print(subcommand_header(subp.prog))
            subp.print_help()

        parser.exit()

    args = parser.parse_args()

    args.func(args)


if __name__ == '__main__':
    main()
