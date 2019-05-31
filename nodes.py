#!/usr/bin/env python
import os
import xml.etree.ElementTree as Et
from collections import defaultdict
from datetime import datetime
from subprocess import Popen, PIPE

HOME = os.getenv("HOME")
USER = os.getenv("USER")
WIDTH = os.getenv("COLUMNS")

if not WIDTH:
    with os.popen('stty size', 'r') as ttyin:
        _, WIDTH = map(int, ttyin.read().split())

UP_STATES = {"job-exclusive", "job-sharing", "reserve", "free", "busy", "time-shared"}


class NodeStatusError(Exception):
    """Custom error thrown by nodestatus code"""


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
        if 'log_start_time' in self:
            return self['log_start_time'].strftime('%Y-%m-%d %H:%M:%S')
        return ''

    @property
    def runtime(self):
        if 'Resource_List.walltime' in self:
            return '%s/%sh' % (
                self.get('resources_used.walltime', '00:00:00').split(':')[0],
                self['Resource_List.walltime'].split(':')[0]
            )
        elif 'walltime' in self:
            return self['walltime']
        return ''

    @property
    def rmem(self):
        return float(self.get('Resource_List.mem', '0mb')[:-2]) / 1024

    @property
    def memory(self):
        if 'Resource_List.mem' in self:
            mem = float(self.get('resources_used.mem', '0kb')[:-2]) / (1024 * 1024)
            rmem = float(self.get('Resource_List.mem', '0mb')[:-2]) / 1024

            return '%.1f/%.1fG (%3d%%)' % (mem, rmem, mem / rmem * 100)
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

    def __getitem__(self, item):
        if isinstance(item, slice):
            return sorted(self.values(), key=lambda x: x.job_id, reverse=True)[item].__iter__()

        return super(JobList, self).__getitem__(item)

    def __iter__(self):
        return sorted(self.values(), key=lambda x: x.job_id, reverse=True).__iter__()


def read_qstatx(jobs=None):
    """Parse qstat -x output to get the most details about queued/running jobs of the user that executes this script.
    Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most useful ones are:
    resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...
    This is the XML parsing version. Should be a bit safer than parsing regular output with RE.

    :return: Parsed jobs from qstat output
    :rtype: dict
    """
    proc = Popen('qstat -x', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run qstat: %s" % err)

    if not qstat:
        return {}

    jobs = jobs if jobs is not None else JobList()

    root = Et.fromstring(qstat)
    for job_ele in root:
        job = {attr.tag: attr.text for attr in job_ele}
        for ts in ['qtime', 'mtime', 'ctime', 'etime']:
            if ts in job:
                job[ts] = datetime.fromtimestamp(int(job[ts]))

        for sub in ['Resource_List', 'resources_used']:
            if sub in job:
                job.pop(sub)
                for rl in job_ele.find(sub):
                    job['%s.%s' % (sub, rl.tag)] = rl.text

        jobs[job['Job_Id']] = job

    job_map = defaultdict(list)
    for job in jobs:
        execs = job.get('exec_host', '').split('+')
        job['execs'] = execs
        job_map[execs[0].split('/')[0]].append(job)

    return job_map


def read_nodes():
    """

    :return:
    :rtype: dict
    """
    proc = Popen('pbsnodes -x', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)
    pbsnodes, err = proc.communicate()
    if err:
        raise Exception("Can't run pbsnodes: %s" % err)

    nodes = {}

    root = Et.fromstring(pbsnodes)
    for node_ele in root:
        node = {attr.tag: attr.text for attr in node_ele}
        node['state'] = node['state'].split(',')

        if 'status' in node:
            for k, v in [kv.split('=') for kv in node['status'].split(',')]:
                if k not in node:
                    node[k] = v
            node.pop('status')

        node['up'] = len(UP_STATES.intersection(node['state'])) > 0
        nodes[node['name']] = node

    return nodes


def check_status(args):
    """Print job details for current user. Output format can be fine-tuned with args argument.

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    job_map = read_qstatx()
    nodes = read_nodes()

    headers = ('Node', 'Status', 'Load', 'Used cores', 'Used memory', 'Jobs')
    output = []

    for node, node_data in sorted(nodes.items()):  # , key=lambda x: (not x[1]['up'], x[1]['state'], x[0])
        node_name = node.split('.')[0]
        jobs = job_map.get(node, [])
        job_mem = sum([job.rmem for job in jobs])

        node_jobs = []
        if 'jobs' in node_data:
            node_jobs = node_data.get('jobs', '').split(',')
        phys_mem = int(node_data.get('physmem', '0kb')[:-2]) / 1024. / 1024.
        all_cpus = int(node_data.get('ncpus', '0'))

        row = [
            node_name,
            ','.join(node_data['state']),
            node_data['loadave'],  # Load
            "%3d/%3d (%3d%%)" % (len(node_jobs), all_cpus, 1. * len(node_jobs) / all_cpus * 100.),  # Cores
            "%5.1f/%5.1fG (%3d%%)" % (job_mem, phys_mem, job_mem / phys_mem * 100.),  # Memory
        ]

        if args.print_job_details:
            for jidx, job in enumerate(jobs):
                job_val = '%s %s walltime=%s memory=%s cores=%s' % (
                    job['euser'],
                    job['job_id'],
                    job.runtime,
                    job.memory,
                    job['Resource_List.nodes'].split('=')[-1]
                )

                if jidx == 0:
                    output.append(row + [job_val])
                else:
                    output.append(([''] * len(row)) + [job_val])
        else:
            output.append(row + [','.join([str(j.job_id) for j in jobs])])

    # Printing bits
    column_sizes = [0] * len(headers)
    for output_row in output:
        for cidx, cell in enumerate(output_row):
            column_sizes[cidx] = max(column_sizes[cidx], len(cell))

    free_space = max(WIDTH - (sum(column_sizes) + (3 * (len(column_sizes) - 1))), 0)

    column_sizes[-1] += free_space

    header_format = ' | '.join(['%%-%ds' % s for s in column_sizes])
    # Align last column to the left, the rest are to the right
    row_format = ' | '.join(
        ['%%%ds' % (s if sidx != len(column_sizes) - 1 else -s) for sidx, s in enumerate(column_sizes)])
    header = header_format % headers

    print('-' * len(header))
    for output_row in output:
        print(row_format % tuple(output_row))


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Check nodes status.')
    parser.add_argument('-d', '--list-job-details', dest='print_job_details', action='store_true',
                        help='List jobs running on nodes')
    args = parser.parse_args()

    try:
        check_status(args)
    except NodeStatusError as e:
        # Fail gracefully only for known errors
        parser.error(str(e))


if __name__ == '__main__':
    main()
