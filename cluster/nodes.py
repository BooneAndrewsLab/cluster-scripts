#!/usr/bin/env python
from collections import defaultdict

from cluster.common import Cluster
from cluster.tools import print_table


def check_status(args):
    """ Print node details

    :param args: Arguments from argparse
    :type args: argparse.Namespace
    """
    cluster = Cluster(jobs_qstat=True, nodes=True, link=True)
    nodes = []

    if args.filter_states:
        cluster.filter_node_states(set(args.filter_states.lower().split(',')))

    for node in cluster.nodes:
        nodes.append([
            node.name,
            node.states,
            node.load,
            "%3d/%3d (%3d%%)" % (
                node.cpu_res, node.cpu_all, 1. * node.cpu_res / node.cpu_all * 100.) if node.cpu_all else 'N/A',  # Cores
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
