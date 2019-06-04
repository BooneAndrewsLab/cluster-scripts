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


class Node:
    def __init__(self, nodeele):
        node = {attr.tag: attr.text for attr in nodeele}
        status = dict([kv.split('=') for kv in node['status'].split(',')]) if 'status' in node else {}
        print(status)

    @property
    def name(self):
        return self['name'].split('.')[0]

    @property
    def res_cpus(self):
        return sum([len(job['execs']) for job in self['jobs']])

    @property
    def all_cpus(self):
        return int(self.get('np', '0'))

    @property
    def states(self):
        return set(self['state'].split(','))

    @property
    def up(self):
        return len(UP_STATES.intersection(self.states)) > 0

    @property
    def res_mem(self):
        return sum([job.rmem for job in self['jobs']])

    @property
    def all_mem(self):
        return int(self.get('physmem', '0kb')[:-2]) / 1024. / 1024.


class Job:
    def __init__(self, jobele):
        job = {attr.tag: attr.text for attr in jobele}
        self.job_id = job['Job_Id'].split('.')[0]
        self.user = job['euser']
        self.node = None
        if job.get('exec_host'):
            self.node = job['exec_host'].split('.')[0]


def read_xml(cmd):
    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run %s: %s" % (cmd, err))

    if not qstat:
        return []

    return Et.fromstring(qstat)


def read_qstatx():
    """Parse qstat -x output to get the most details about queued/running jobs of the user that executes this script.
    Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most useful ones are:
    resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...
    This is the XML parsing version. Should be a bit safer than parsing regular output with RE.

    :return: Parsed jobs from qstat output
    :rtype: dict
    """
    jobs = {}

    root = read_xml('qstat -x')
    for job in map(Job, root):
        jobs[job.job_id] = job

    return jobs


def read_nodes(jobs):
    """
    :return:
    :rtype: list
    """
    root = read_xml('pbsnodes -x')
    for node in map(Node, root):
        pass

    # for node_ele in root:
    #     node = Node({attr.tag: attr.text for attr in node_ele})
    #
    #     if 'status' in node:
    #         for k, v in [kv.split('=') for kv in node['status'].split(',')]:
    #             if k not in node:
    #                 node[k] = v
    #         node.pop('status')
    #
    #     job_list = jobs.get(node['name'], [])
    #     if node['jobs']:
    #         for job in node['jobs'].split(','):
    #             print(job)
    #             job_id = int(job.split('/')[1].split('.')[0])
    #             if job_id not in jobs:
    #                 node.setdefault('orphans', set()).add(job_id)
    #             else:
    #                 job_list.append(jobs[job_id])
    #
    #     node['jobs'] = job_list
    #
    #     nodes.append(node)
    #
    # return sorted(nodes, key=lambda n: n.name)


def check_status(args):
    """Print job details for current user. Output format can be fine-tuned with args argument.

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    job_map = read_qstatx()
    node_list = read_nodes(job_map)
    nodes = []

    if args.filter_states:
        states = args.filter_states.lower().split(',')
        if 'down' in states:
            node_list = filter(lambda x: not x.up, node_list)
        else:
            node_list = filter(lambda x: x.states.intersection(states), node_list)

    for node in node_list:
        nodes.append([
            node.name,
            node.get('state', 'N/A'),
            node.get('loadave', '0'),
            "%3d/%3d (%3d%%)" % (node.res_cpus, node.all_cpus, 1. * node.res_cpus / node.all_cpus * 100.),  # Cores
            "%5.1f/%5.1fG (%3d%%)" % (
                node.res_mem, node.all_mem, node.res_mem / node.all_mem * 100.) if node.all_mem else 'N/A',  # Memory
            ''.join(('*' * node.res_cpus) + ('-' * (node.all_cpus - node.res_cpus)))
        ])

        if args.show_job_owners:
            nodes[-1][-1] = ''
            empty = [''] * 5

            users = defaultdict(list)
            for job in node['jobs']:
                users[job['euser']].append(job)
            for orphan in node.get('orphans', []):
                users['ORPHANS'].append(orphan)

            for idx, uitem in enumerate(users.items()):
                u, jobs = uitem
                column_data = '%s: %s' % (u, ' '.join([str(j.job_id) for j in jobs]))

                if idx:
                    nodes.append(empty + [column_data])
                else:
                    nodes[-1][-1] = column_data

    # Printing bits
    headers = ('Node', 'Status', 'Load', 'Used cores', 'Used memory', 'Jobs')
    sizes = [max(map(len, col)) for col in zip(headers, *nodes)]  # Find optimal column size
    columns = ['%%-%ds' % s for s in sizes]

    # Pad last column with leftover space
    out_len = ' | '.join(columns[:-1]) % headers[:-1]
    free_space = max(32, WIDTH - 3 - len(out_len))
    columns[-1] = '%%-%ds' % free_space

    columns_format = ' | '.join(columns)
    header = columns_format % headers
    print(header)
    print('=' * len(header))

    for node in nodes:
        print(columns_format % tuple(node))


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Check nodes status.')
    parser.add_argument('-o', '--show-job-owners', action='store_true', help='List jobs running on nodes')
    parser.add_argument('-s', '--filter-states', help='Display only nodes in FILTER_STATES (comma separated). '
                                                      'Can use "DOWN" for any offline state.')
    args = parser.parse_args()

    try:
        check_status(args)
    except NodeStatusError as e:
        # Fail gracefully only for known errors
        parser.error(str(e))


if __name__ == '__main__':
    main()
