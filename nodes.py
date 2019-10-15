#!/usr/bin/env python
import os
import re
import xml.etree.ElementTree as Et
from collections import defaultdict
from subprocess import Popen, PIPE

# Get terminal width for nicer printing
WIDTH = os.getenv("COLUMNS")

if not WIDTH:
    with os.popen('stty size', 'r') as ttyin:
        _, WIDTH = map(int, ttyin.read().split())

# Some useful constants, python 2.6 compatible
UP_STATES = set(("job-exclusive", "job-sharing", "reserve", "free", "busy", "time-shared"))
RE_JOB = re.compile(r'(\d+/)?(\d+)[.].+')
# Adapted from: https://stackoverflow.com/a/14693789
ANSI_ESC = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


class Node:
    jobs_qstat = []
    orphans = []
    mem_res = 0

    def __init__(self, nodeele):
        """ Object representing one node, as parsed from pbsnodes output

        :param nodeele: Node details from pbsnodes
        :type nodeele: Et.Element
        """
        node = dict([(attr.tag, attr.text) for attr in nodeele])  # python 2.6 compat
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
        """ Iterate through the job list and adopt jobs that are executing on this nodes.

        :param jobs: Jobs read from qstat
        :type jobs: dict[str, Job]
        :return: Reference to this node for chaining purposes.
        :rtype: Node
        """
        self.jobs_qstat = [j for j in jobs.values() if j.node == self.name]
        self.mem_res = sum([j.mem for j in self.jobs_qstat])
        self.orphans = [jobs[j] for j in self.jobs_node if not jobs[j].node]
        return self


class Job:
    def __init__(self, jobele):
        """ Object representing one Job as parsed from qstat output

        :param jobele: Job details from qstat
        :type jobele: Et.Element
        """
        job = dict([(attr.tag, attr.text) for attr in jobele])  # python 2.6 compat
        resources = dict([(r.tag, r.text) for r in jobele.find('Resource_List')])  # python 2.6 compat

        self.job_id = job['Job_Id'].split('.')[0]
        self.user = job['euser']
        self.mem = int(resources.get('mem', '2097152kb')[:-2]) / 1024.  # 2GB default memory
        self.node = None
        if job.get('exec_host'):
            self.node = job['exec_host'].split('.')[0]


def read_xml(cmd):
    """ Execute cmd and parse the output XML

    :param cmd: Command to run
    :type cmd: str
    :return: List of children elements in xml root
    :rtype: list[Et.Element]
    """
    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)
    qstat, err = proc.communicate()
    if err:
        raise Exception("Can't run %s: %s" % (cmd, err))

    if not qstat:
        return []

    return Et.fromstring(ANSI_ESC.sub('', qstat))  # trim any color escape sequences returned by our command


def read_qstat():
    """ Parse qstat -x output to get the most details about queued/running jobs of the user that executes this script.

    :return: Parsed jobs from qstat output
    :rtype: dict[str, Job]
    """
    return dict([(j.job_id, j) for j in map(Job, read_xml('/usr/bin/qstat -x'))])  # python 2.6 compat


def read_nodes(jobs):
    """ Parse pbsnodes -x output to get node details.

    :return: List of nodes
    :rtype: list[Node]
    """
    return sorted([node.grab_own_jobs(jobs) for node in map(Node, read_xml('pbsnodes -x'))], key=lambda n: n.name)


def print_table(headers, data):
    """ Print a table in terminal, properly padded

    :param headers: Table headers
    :param data: Table data
    :type headers: list[str]
    :type data: list[list]
    """
    sizes = [max(map(len, col)) for col in zip(headers, *data)]  # Find optimal column size
    columns = ['%%-%ds' % s for s in sizes]

    # Pad last column with leftover space
    out_len = ' | '.join(columns[:-1]) % tuple(headers[:-1])
    free_space = max(32, WIDTH - 3 - len(out_len))
    columns[-1] = '%%-%ds' % free_space
    columns_format = ' | '.join(columns)
    header = columns_format % tuple(headers)

    print(header)
    print('=' * len(header))

    for node in data:
        print(columns_format % tuple(node))


def check_status(args):
    """ Print node details

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    job_map = read_qstat()
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
    print_table(['Node', 'Status', 'Load', 'Used cores', 'Used memory', 'Jobs'], nodes)


def main():
    """ Execute main program
    """
    # noinspection PyCompatibility
    import argparse
    parser = argparse.ArgumentParser(description='Check nodes status.')
    parser.add_argument('-o', '--show-job-owners', action='store_true', help='List jobs running on nodes')
    parser.add_argument('-s', '--filter-states', help='Display only nodes in FILTER_STATES (comma separated).')
    args = parser.parse_args()

    check_status(args)


if __name__ == '__main__':
    main()
