#!/usr/bin/env python

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
    pass


def read_pbs_output():
    res = {}

    for out in os.listdir(PBS_PATH):
        if not out.endswith('.bc.ccbr.utoronto.ca.OU'):
            continue

        job_id = out[:-3]  # remove .OU

        out_data = {'finished': datetime.fromtimestamp(os.path.getctime(os.path.join(PBS_PATH, out)))}
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


def read_pbs_log():
    res = {}

    if os.path.isfile(LOG_PATH):
        with open(LOG_PATH) as log:
            for l in log:
                timestamp, job_id, cmd = l.strip().split(None, 2)
                res[job_id] = (datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S.%f]"), cmd)

    return res


def read_qstat_detailed():
    proc = Popen('qstat -f', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    jobs = {}

    job_re = re.compile("Job Id:[\s\S]*?(?=\nJob Id:|$)")
    job_param_re = re.compile('[ ]{4}[\s\S]*?(?=\n[ ]{4}|$)')

    for job in job_re.findall(qstat):
        job_id = job[8:job.index('\n')]
        job_data = dict([kv.strip().replace('\n\t', '').split(' = ') for kv in job_param_re.findall(job)])
        if job_data['euser'] == USER:
            jobs[job_id] = job_data

    return jobs


def read_qstat():
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


def _parse_timearg(arg, since=datetime.now()):
    amount = int(arg[:-1])
    period = arg[-1]

    return since - timedelta(
        **{{'h': 'hours', 'd': 'days', 'w': 'weeks'}[period]: amount}
    )


def print_all_jobs():
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
    failed_check = None
    failed_value = None
    if args.failed_since:
        if re.match('\d{4}-\d{2}-\d{2}', args.failed_since):
            failed_check = 'date'
            failed_value = datetime.strptime(args.failed_since, '%Y-%m-%d')
        elif re.match('^\d+$', args.failed_since):
            failed_check = 'job_id'
            failed_value = int(args.failed_since)
        elif re.match('\d+[hdw]', args.failed_since):
            failed_check = 'date'
            failed_value = _parse_timearg(args.failed_since)
        else:
            raise JobStatusError("Invalid argument to --failed-since")

    qstatf = read_qstat_detailed()
    logged = read_pbs_log()
    output = read_pbs_output()

    columns = ' | '.join(['%-8s', '%-11s', '%-4s', '%-19s', '%-18s', '%-18s', '%-32s'])
    header = columns % ('Job ID', 'Status', 'Exit', 'Start Time', 'Elapsed/Total Time', 'Used Memory', 'Command')

    jobs = sorted(set(qstatf.keys() + logged.keys() + output.keys()), key=lambda x: int(x.split('.')[0]), reverse=True)
    data = []
    for job in jobs:
        job_id = int(job.split('.')[0])

        job_qstat = qstatf.get(job, {})
        job_output = output.get(job, {})
        start, cmd = logged.get(job, ('', ''))

        status = job_qstat.get('job_state', 'Completed' if job_output else '?')
        if 'queue' in job_qstat:
            status += ' (%s)' % job_qstat['queue']

        exit_status = job_output.get('Exit status', '-')
        if exit_status not in ('-', '0'):
            status = 'Failed'

        if start:
            start = start.strftime('%Y-%m-%d %H:%M:%S')

        time = ''
        mem = ''

        if job_qstat:
            time = '%s/%s' % (
                job_qstat.get('resources_used.walltime', '00:00:00'), job_qstat.get('Resource_List.walltime'))

            mem = float(job_qstat.get('resources_used.mem', '0kb')[:-2]) / (1024 * 1024)
            rmem = float(job_qstat.get('Resource_List.mem', '0mb')[:-2]) / 1024

            mem = '%.1f/%.1fG (%3d%%)' % (mem, rmem, mem / rmem)
        elif job_output:
            time = job_output['walltime']
            mem = job_output['mem']

        cmd = (job_output.get('Run command') or cmd.strip('"') or '-')
        if len(cmd) > 32:
            cmd = cmd[:29] + '...'

        if failed_check:
            if status == 'Failed':
                if failed_check == 'date' and job_output['finished'] >= failed_value:
                    data.append(cmd)
                elif failed_check == 'job_id' and job_id >= failed_value:
                    data.append(cmd)
        else:
            data.append((job_id, status, exit_status, start, time, mem, cmd))

    if failed_check:
        for cmd in data:
            print(cmd)
    else:
        print(header)
        print('-' * len(header))
        for row in data:
            print(columns % row)



def archive(_):
    print('ARCHIVE')


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
    details_parser.add_argument('-f', '--failed-since', help='Print all failed commands after FAILED_SINCE.'
                                                             'Must be either a date (YYYY-MM-DD) or Job ID (numeric part).')
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
        parser.error(str(e))


if __name__ == '__main__':
    main()
