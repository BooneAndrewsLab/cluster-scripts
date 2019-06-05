#!/usr/bin/env python
import os
import re
import xml.etree.ElementTree as Et
from collections import defaultdict
from subprocess import Popen, PIPE

WIDTH = os.getenv("COLUMNS")

if not WIDTH:
    with os.popen('stty size', 'r') as ttyin:
        _, WIDTH = map(int, ttyin.read().split())

UP_STATES = {"job-exclusive", "job-sharing", "reserve", "free", "busy", "time-shared"}
RE_JOB = re.compile(r'(\d+/)?(\d+)[.].+')


class Node:
    jobs_qstat = []
    orphans = []
    mem_res = 0

    def __init__(self, nodeele):
        node = {attr.tag: attr.text for attr in nodeele}
        status = dict([kv.split('=') for kv in node['status'].split(',')]) if 'status' in node else {}

        self.name = node['name'].split('.')[0]
        jobs = [RE_JOB.match(j).group(2) for j in node.get('jobs', '').split(',') if RE_JOB.match(j)]
        self.jobs_node = set(jobs)

        self.cpu_all = int(node.get('np', '0'))
        self.cpu_res = len(jobs)

        self.mem_all = int(status.get('physmem', '0kb')[:-2]) / 1024. / 1024.
        self.load = status.get('loadave', '0')

        self.states = node.get('state', 'N/A')
        self.state_set = set(self.states.split(','))
        self.is_up = len(UP_STATES.intersection(self.state_set)) > 0

    def grab_own_jobs(self, jobs):
        self.jobs_qstat = [j for j in jobs.values() if j.node == self.name]
        self.mem_res = sum([j.mem for j in self.jobs_qstat])
        self.orphans = [jobs[j] for j in self.jobs_node if not jobs[j].node]
        return self


class Job:
    def __init__(self, jobele):
        job = {attr.tag: attr.text for attr in jobele}
        resources = {r.tag: r.text for r in jobele.find('Resource_List')}

        self.job_id = job['Job_Id'].split('.')[0]
        self.user = job['euser']
        self.mem = int(resources['mem'][:-2]) / 1024.
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
    return sorted([node.grab_own_jobs(jobs) for node in map(Node, root)], key=lambda n: n.name)


def check_status(args):
    """Print job details for current user. Output format can be fine-tuned with args argument.

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    job_map = read_qstatx()
    node_list = read_nodes(job_map)
    nodes = []

    if args.filter_states:
        states = set(args.filter_states.lower().split(','))
        node_list = filter(lambda x: not states.difference(x.state_set), node_list)

    for node in node_list:
        nodes.append([
            node.name,
            node.states,
            node.load,
            "%3d/%3d (%3d%%)" % (node.cpu_res, node.cpu_all, 1. * node.cpu_res / node.cpu_all * 100.),  # Cores
            "%5.1f/%5.1fG (%3d%%)" % (
                node.mem_res, node.mem_all, node.mem_res / node.mem_all * 100.) if node.mem_all else 'N/A',  # Memory
            ''.join(('*' * node.cpu_res) + ('-' * (node.cpu_all - node.cpu_res)))
        ])

        if args.show_job_owners:
            nodes[-1][-1] = ''
            empty = [''] * 5

            users = defaultdict(list)
            for job in node.jobs_qstat:
                users[job.user].append(job)
            for orphan in node.orphans:
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
    parser.add_argument('-s', '--filter-states', help='Display only nodes in FILTER_STATES (comma separated).')
    args = parser.parse_args()

    check_status(args)


if __name__ == '__main__':
    main()
