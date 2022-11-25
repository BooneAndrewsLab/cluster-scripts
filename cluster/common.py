import json
import os
import sys
from collections import defaultdict
from datetime import datetime

from cluster.config import RE_JOB, UP_STATES, USER, LOG_PATH, PBS_OUTPUT, RE_DC, CLUSTER_NAME
from cluster.tools import read_xml, generic_to_gb, parse_xml, cache_cmd, run_cmd


class Node:
    jobs_qstat = []
    orphans = []
    mem_res = 0

    def __init__(self, node):
        """ Object representing one node, as parsed from pbsnodes output

        :param nodeele: Node details from pbsnodes
        :type nodeele: Et.Element
        """
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
        :type jobs: dict[Job]
        :return: Reference to this node for chaining purposes.
        :rtype: Node
        """
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
    user = None

    # Variables to print
    name = ''
    state = '?'
    exit_status = '-'
    start = ''
    runtime = ''
    memory = ''
    cmd = ''

    def parse_qstat(self, job):
        """ Object representing one Job as parsed from qstat output

        :param job: Job details from qstat
        :type job: dict
        """
        self.job_id = int(job['Job_Id'].split('.')[0])
        self.user = job['euser']
        if 'Resource_List.mem' in job:
            self.mem = generic_to_gb(job['Resource_List.mem'])

        if job.get('exec_host'):
            self.node = job['exec_host'].split('/')[0].split('.')[0]

        self.state = job.get('job_state', self.state)
        if 'queue' in job:
            self.state += ' (%s)' % job['queue']

        if 'Resource_List.walltime' in job:
            self.runtime = '%s/%s' % (job.get('resources_used.walltime', '00:00:00'), job['Resource_List.walltime'])

        self.name = job.get('Job_Name', self.name)

        used_mem = generic_to_gb(job.get('resources_used.mem', '0gb'))
        self.memory = '%.1f/%.1fG (%3d%%)' % (used_mem, self.mem, used_mem / self.mem * 100)
        self.qstat = True

        if 'stime' in job:
            self.start_time = job['stime']
            self.start = self.start_time.strftime('%Y-%m-%d %H:%M:%S')

    def parse_pbs_log(self, job_id, start_time, cmd, log_line):
        """ Parse this job from $HOME/.pbs_log

        :param job_id: Job ID
        :param start_time: Time when the job was submitted
        :param cmd: Submitted command
        :param log_line: Entire line from .pbs_log
        :type job_id: str
        :type start_time: datetime
        :type cmd: str
        :type log_line: str
        """
        self.job_id = int(job_id)
        self.user = USER
        self.start_time = start_time
        self.start = start_time.strftime('%Y-%m-%d %H:%M:%S')
        self.cmd = cmd[1:-1]
        self.pbs_log = log_line

    def parse_pbs_output(self, output):
        """ Parse this job from $HOME/pbs-output

        :param output: Parsed output file
        :type output: dict
        """
        self.job_id = int(output['job_id'])
        self.user = USER
        self.exit_status = output.get('Exit status', self.exit_status)
        self.finished = output.get('finished')

        if self.exit_status not in ('-', '0'):
            self.state = 'Failed'
        else:
            self.state = 'Completed'

        if not self.cmd:
            self.cmd = output.get('Run command', '-')

        # Our new output file contains also requested resources, use them for extra display info
        if 'name' in output:
            self.name = output['name'].strip("'")

        self.runtime = output.get('walltime', self.runtime)
        if 'rwalltime' in output and self.runtime:
            rwalltime_str = float(output['rwalltime'])
            rwalltime_str = '%02d:%02d:00' % (rwalltime_str, 60 * (rwalltime_str % 1))
            self.runtime = '%s/%s' % (self.runtime, rwalltime_str)

        self.memory = output.get('mem', self.memory)
        if 'rmem' in output and self.memory:
            rmem = float(output['rmem'])
            rmem = 1024 * 1024 * rmem
            self.memory = '%s/%dkb' % (self.memory[:-2], rmem)

        self.pbs_output = output['pbs_output']

    def __str__(self):
        return str(self.job_id)


class Cluster:
    jobs = defaultdict(Job)
    nodes = []

    def __init__(self, nodes=False, link=False, jobs_qstat=False, jobs_log=False, jobs_pbs=False, cached=True,
                 own=False):
        """

        :param jobs_qstat: Load jobs from qstat
        :param nodes: Load nodes
        :param link: Link jobs to nodes
        :type jobs_qstat: bool
        :type nodes: bool
        :type link: bool
        """
        self.cached = cached

        if not own and (jobs_log or jobs_pbs):  # Restrict reading only own jobs if parsing also log or pbs
            own = True

        if nodes:
            self.load_nodes()

        if jobs_qstat:
            try:
                self.read_qstatj(not own)
            except Exception:  # There is no JSON format option
                self.read_qstatx(not own)

        if jobs_log:
            self.read_pbs_log()

        if jobs_pbs:
            self.read_pbs_output()

        if link:
            self.link_jobs_to_nodes()

    def read_qstatj(self, read_all):
        """Parse qstat -f -F json output to get the most details about queued/running jobs of the user that executes
        this script. Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most
        useful ones are: resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...
        This is the JSON parsing version. Should be a bit safer than parsing regular output with RE.
        """
        job_json = json.loads(cache_cmd('/usr/bin/qstat -f -F json', ignore_cache=not self.cached)).get('Jobs', [])

        for jobid, job in job_json.items():
            job['Job_Id'] = jobid.split('.')[0]
            job['euser'] = job['Job_Owner'].split('@')[0]

            if read_all or job.get('euser') == USER:
                for ts in ['qtime', 'mtime', 'ctime', 'etime', 'stime']:
                    if ts in job:
                        # "Tue Nov 22 12:18:12 2022"
                        job[ts] = datetime.strptime(job[ts], '%a %b %d %H:%M:%S %Y')

                if 'Resource_List' in job:
                    for resource, res_value in job['Resource_List'].items():
                        job['Resource_List.%s' % resource] = res_value

                if 'resources_used' in job:
                    for resource, res_value in job['resources_used'].items():
                        job['resources_used.%s' % resource] = res_value

                self.jobs[job['Job_Id']].parse_qstat(job)

    def read_qstatx(self, read_all):
        """Parse qstat -x output to get the most details about queued/running jobs of the user that executes this
        script. Returns job_id -> job_details pairs. There are too many job_details keys to list here, the most useful
        ones are: resources_used.walltime, Resource_List.walltime, resources_used.mem, Resource_List.mem, ...
        This is the XML parsing version. Should be a bit safer than parsing regular output with RE.
        """
        for jobele in parse_xml(cache_cmd('/usr/bin/qstat -x', ignore_cache=not self.cached)):
            job = dict([(attr.tag, attr.text) for attr in jobele])
            job['Job_Id'] = job['Job_Id'].split('.')[0]

            if read_all or job.get('euser') == USER:
                for ts in ['qtime', 'mtime', 'ctime', 'etime']:
                    if ts in job:
                        job[ts] = datetime.fromtimestamp(int(job[ts]))

                if 'Resource_List' in job:
                    job.pop('Resource_List')
                    for rl in jobele.find('Resource_List'):
                        job['Resource_List.%s' % rl.tag] = rl.text

                if 'resources_used' in job:
                    job.pop('resources_used')
                    for rl in jobele.find('resources_used'):
                        job['resources_used.%s' % rl.tag] = rl.text

                self.jobs[job['Job_Id']].parse_qstat(job)

    def read_pbs_log(self):
        """Parse .pbs_log file created by the new submitjob script for some extra info on running/finished jobs. Returns
        job_id -> (timestamp, command) pairs.
        """
        if os.path.isfile(LOG_PATH):
            with open(LOG_PATH) as log:
                for log_line in log:
                    timestamp, job_id, cmd = log_line.strip().split(None, 2)

                    if CLUSTER_NAME not in job_id:
                        continue

                    job_id = job_id.split('.')[0]
                    try:
                        start_time = datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S.%f]")
                    except ValueError:
                        start_time = datetime.strptime(timestamp, "[%Y-%m-%dT%H:%M:%S]")

                    self.jobs[job_id].parse_pbs_log(job_id, start_time, cmd, log_line)

    def read_pbs_output(self):
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
        TODO: parse contents only if the job is displayed or filtered
        """
        output_files = os.listdir(PBS_OUTPUT)
        if len(output_files) > 1000:
            sys.stderr.write("WARNING: pbs-output folder contains %d files which will make jobstatus details slow. "
                             "We suggest archiving old jobs using 'jobstatus archive' command. See jobstatus archive "
                             "--help to find out how to use it.\n" % (len(output_files),))

        for out in output_files:
            name = ''

            # Parse only job files ending with:
            if out.endswith('%s.OU' % CLUSTER_NAME):  # Read only output for this cluster, if home folder is shared
                job_id = out.split('.')[0]
            elif RE_DC.match(out):  # new DC cluster format... ie: python.o70
                matcher = RE_DC.match(out)
                name = matcher.group(1)
                job_id = matcher.group(2)
            else:
                continue

            # Set ctime of the output file as execution end time
            out_data = {
                'job_id': job_id,
                'finished': datetime.fromtimestamp(os.path.getctime(os.path.join(PBS_OUTPUT, out))),
                'pbs_output': os.path.join(PBS_OUTPUT, out),
                'name': name}

            with open(os.path.join(PBS_OUTPUT, out)) as fin:
                for line in fin:
                    if line.startswith('==>'):  # Parse only useful details, ignore job output for now
                        param, val = line[4:].strip().split(':', 1)
                        param = param.strip()

                        if param == 'Resources used':
                            out_data.update([v.split('=') for v in val.strip().split(',')])
                        elif param == 'Job config':
                            out_data.update([v.split('=') for v in val.strip().split(',')])
                        else:
                            out_data[param] = val.strip()

            self.jobs[job_id].parse_pbs_output(out_data)

    def load_nodes(self):
        """ Parse pbsnodes -x output to get node details.
        """
        self.nodes = []
        try:
            for nodeele in read_xml('pbsnodes -x'):
                self.nodes.append(Node(dict([(attr.tag, attr.text) for attr in nodeele]))) # python 2.6 compat
        except:
            nodes_json = json.loads(run_cmd('pbsnodes -a -F json'))
            for node_id, node_data in nodes_json['nodes'].items():
                node_data['name'] = node_id
                node_data['np'] = node_data['resources_available']['ncpus']
                node_data['status'] = '='.join(['physmem', node_data['resources_available']['mem']])
                node_data['jobs'] = ','.join(node_data.get('jobs', []))
                self.nodes.append(Node(node_data))

        self.nodes = sorted(self.nodes, key=lambda n: ('offline' in n.state_set, n.name))

    def link_jobs_to_nodes(self):
        for node in self.nodes:
            node.grab_own_jobs(self.jobs)

    def filter_node_states(self, states):
        self.nodes = list(filter(lambda x: not states.difference(x.state_set), self.nodes))

    def jobs_list(self):
        return sorted(self.jobs.values(), key=lambda x: x.job_id, reverse=True)


def list_node_names():
    return [n.raw['name'] for n in Cluster(nodes=True).nodes]
