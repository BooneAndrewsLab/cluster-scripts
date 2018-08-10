#!/usr/bin/env python

import os
import re
from collections import defaultdict
from datetime import datetime
from subprocess import Popen, PIPE

HOME = os.getenv("HOME")
USER = os.getenv("USER")

LOG_PATH = os.path.join(HOME, '.pbs_log')
PBS_PATH = os.path.join(HOME, 'pbs-output')
USER_LABEL = '*%s' % (USER,)


def read_output():
    res = {}

    for out in os.listdir(PBS_PATH):
        if not out.endswith('.bc.ccbr.utoronto.ca.OU'):
            continue

        job_id = out[:11]

        out_data = {}
        with open(os.path.join(PBS_PATH, out)) as fin:
            for line in fin:
                if line.startswith('==>'):
                    param, val = line[4:].strip().split(':', 1)
                    param = param.strip()

                    if param == 'Resources used':
                        out_data.update([v.split('=') for v in val.strip().split(',')])
                    else:
                        out_data[param] = val.strip()

        res[job_id] = out_data

    return res


def read_log():
    res = {}

    if os.path.isfile(LOG_PATH):
        with open(LOG_PATH) as log:
            for l in log:
                timestamp, job_id, cmd = l.strip().split(None, 2)
                res[job_id[:11]] = (datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S.%f]"), cmd)

    return res


def get_my_jobs():
    proc = Popen('qstat -a -u %s' % (USER, ), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    user_stats = {}

    parse = False
    for line in qstat.split('\n'):
        if not parse or not line:
            if line.startswith('-'):
                parse = True
            continue

        job_id, _user, queue, name, _session, _nodes, _tasks, rmem, rtime, status, time = re.split('\s+', line.strip())

        user_stats[job_id.split('.')[0]] = (name, time, status, queue, rmem, rtime)

    return user_stats


def get_active_jobs():
    proc = Popen('qstat', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    queue_stats = defaultdict(lambda: defaultdict(int))
    total_stats = defaultdict(int)

    for line in qstat.split('\n')[2:]:
        if not line:
            continue

        job_id, name, user, time, status, queue = re.split('\s+', line.strip())

        user = USER_LABEL if user == USER else user
        user_stats[user][queue][status] += 1
        queue_stats[queue][status] += 1
        total_stats[status] += 1

    return user_stats, queue_stats, total_stats


def print_all_jobs(_):
    user_stats, queue_stats, total_stats = get_active_jobs()

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


def details(_):
    qstat = get_my_jobs()
    logged = read_log()
    output = read_output()

    columns = ' | '.join(['%-11s', '%-11s', '%-4s', '%-19s', '%-18s', '%-11s', '%-32s'])
    header = columns % ('Job ID', 'Status', 'Exit', 'Start Time', 'Elapsed/Total Time', 'Used Memory', 'Command')

    print(header)
    print('-' * len(header))

    jobs = sorted(set(logged.keys() + output.keys()), key=lambda x: int(x.split('.')[0]), reverse=True)
    for job in jobs:
        name, time, status, queue, rmem, rtime = qstat.get(job, ('',) * 4)
        job_output = output.get(job, {})
        start, cmd = logged.get(job, ('-', ''))

        if not status:
            status = 'F'
        else:
            status += ' (%s)' % queue

        if start != '-':
            start = start.strftime('%Y-%m-%d %H:%M:%S')

        if not time:
            if job in output:
                time = output[job]['walltime']

        cmd = (job_output.get('Run command') or cmd or '-')
        if len(cmd) > 32:
            cmd = cmd[:29] + '...'

        row = [job, status, job_output.get('Exit status', '-'), start, time, job_output.get('mem', '-'), cmd]

        print(columns % tuple(row))


def archive(_):
    print('ARCHIVE')


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Check job status')

    command_parsers = parser.add_subparsers(title='Available subcommands',
                                            dest='command',
                                            description='For detailed subcommand help run: <subcommand> -h.',
                                            metavar=' ' * 20)  # Used for some nice spacing

    details_parser = command_parsers.add_parser('details', help='Show details of my jobs.')
    details_parser.set_defaults(func=details)

    archive_parser = command_parsers.add_parser('archive', help='Archive finished jobs.')
    archive_parser.add_argument('-a', '--age', default='1m',
                                help='Archive finished jobs older than specified age. '
                                     'Allowed age format is <number><unit> (ie. 2w or 3m or 1d)'
                                     'Unit can be one of: d(days), w(weeks) or m(months)')
    archive_parser.set_defaults(func=archive)

    args = parser.parse_args()

    args.func(args)


if __name__ == '__main__':
    main()
