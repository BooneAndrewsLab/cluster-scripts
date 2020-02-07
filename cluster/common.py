from datetime import datetime

from cluster.config import RE_JOB, UP_STATES
from cluster.tools import read_xml


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
        self.raw = node
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
        :type jobs: list[Job]
        :return: Reference to this node for chaining purposes.
        :rtype: Node
        """
        jobs = dict([(j.job_id, j) for j in jobs])  # Generate job index

        self.jobs_qstat = [j for j in jobs.values() if j.node == self.name]
        self.mem_res = sum([j.mem for j in self.jobs_qstat])
        self.orphans = [jobs[j] for j in self.jobs_node if not jobs[j].node]
        return self


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

    def __init__(self, jobele):
        """ Object representing one Job as parsed from qstat output

        :param jobele: Job details from qstat
        :type jobele: Et.Element
        """
        job = dict([(attr.tag, attr.text) for attr in jobele])  # python 2.6 compat
        resources = dict([(r.tag, r.text) for r in jobele.find('Resource_List')])  # python 2.6 compat

        self.job_id = job['Job_Id'].split('.')[0]
        self.user = job['euser']
        self.mem = int(resources.get('mem', '2048mb')[:-2]) / 1024.  # 2GB default memory
        self.node = None
        if job.get('exec_host'):
            self.node = job['exec_host'].split('.')[0]

        for ts in ['qtime', 'mtime', 'ctime', 'etime']:
            if ts in job:
                job[ts] = datetime.fromtimestamp(int(job[ts]))

    def __str__(self):
        return self.job_id


class Cluster:
    """
    :param jobs: List of jobs
    :param nodes: List of nodes
    :type jobs: list[Job]
    :type nodes: list[Node]
    """

    jobs = []
    nodes = []

    def __init__(self, jobs=False, nodes=False, link=False):
        """

        :param jobs: Load jobs
        :param nodes: Load nodes
        :param link: Link jobs to nodes
        :type jobs: bool
        :type nodes: bool
        :type link: bool
        """
        if jobs:
            self.load_jobs()
        if nodes:
            self.load_nodes()
        if link:
            self.link_jobs_to_nodes()

    def load_jobs(self):
        """ Parse qstat -x output to get the most details about
        queued/running jobs of the user that executes this script.
        """
        self.jobs = list(map(Job, read_xml('/usr/bin/qstat -x')))

    def load_nodes(self):
        """ Parse pbsnodes -x output to get node details.
        """
        self.nodes = sorted(list(map(Node, read_xml('pbsnodes -x'))), key=lambda n: ('offline' in n.state_set, n.name))

    def link_jobs_to_nodes(self):
        for node in self.nodes:
            node.grab_own_jobs(self.jobs)

    def filter_node_states(self, states):
        self.nodes = list(filter(lambda x: not states.difference(x.state_set), self.nodes))

    def filter_job_owner(self, owner):
        self.jobs = list(filter(lambda x: x.user == owner, self.jobs))


def list_node_names():
    return [n.raw['name'] for n in Cluster(nodes=True).nodes]
