import os
import re
import time
from datetime import datetime

from cluster.config import CWD, PBS_OUTPUT, PATH
from cluster.tools import run_cmd, get_job_template


def submit(cmd, walltime=24, mem=2, cpu=1, email=None, wd=CWD, output_dir=PBS_OUTPUT, path=PATH, job_name=None,
           pretend=False, environment=None, conda_profile="/etc/profile.d/conda.sh", node="1", job_template=None):
    """Submits a command to the cluster

    :param cmd: The command to run.
    :param walltime: Requested run-time limit in hours. Default 24hrs.
    :param mem: Requested memory limit in GB. Default 2GB.
    :param cpu: Requested number of CPU. Default 1 CPU.
    :param email: Email address for notifications.
    :param wd: Working directory. Default is cwd().
    :param output_dir: Where to save job output. Default is $HOME/pbs-output
    :param path: Job's PATH. Default is $PATH.
    :param job_name: Name of the job as displayed by qstat. Default is command name, ie: awk
    :param pretend: Don't submit job to qsub, just print it out instead
    :param environment: Name of the conda environment to activate
    :param node: Name of the node to use, "1" - any
    :param job_template: PBS job template
    :type cmd: str
    :type walltime: float
    :type mem: float
    :type cpu: int
    :type email: str
    :type wd: str
    :type output_dir: str
    :type path: str
    :type job_name: str
    :type pretend: bool
    :type environment: str
    :type node: str
    :type job_template: string.Template
    :return: Job id returned by qsub.
    :rtype: str
    """

    # Create output dir if it does not exist yet
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    walltime_str = '%02d:%02d:00' % (walltime, 60 * (walltime % 1))
    memory = '%dM' % (1024 * mem,)
    send_email = 'ae'

    if not email:
        send_email = 'n'

    resources = ['walltime=%s' % (walltime_str,), 'mem=%s' % (memory,), 'nodes=%s:ppn=%d' % (node, cpu,)]
    resources = ','.join(resources)

    cmd_echo = cmd.replace('$', r'\$').replace('"', r'\"')

    if not job_name:
        job_name = cmd.split()[0]  # Remove anything following a space (can be introduced during smart quoting)
        job_name = os.path.split(job_name)[-1]  # Remove the path before any command
        job_name = job_name.replace('&', '')  # Remove any ampersands
        job_name = re.sub(r'^\d+', '', job_name)  # Remove any leading digits, otherwise qsub will throw an error

    job_setup = ''
    if environment and conda_profile:
        job_setup = """source %s
conda activate %s""" % (conda_profile, environment)

    exposed_config = [
        ('rwalltime', walltime),
        ('rmem', mem),
        ('rcpu', cpu),
        ('name', job_name),
        ('conda_environment', environment),
        ('wd', wd)
    ]
    # this is a more human readable format than json
    job_config = ','.join("%s=%r" % item for item in exposed_config if item[1])

    if not job_template:
        # Grab default template if None
        job_template = get_job_template()

    pbs = job_template.safe_substitute(
        pbs_output=output_dir,
        resources=resources,
        name=job_name,
        cwd=wd,
        path=path,
        cpu=cpu,
        send_email=send_email,
        email=email,
        cmd_echo=cmd_echo,
        job_setup=job_setup,
        job_config=job_config,
        cmd=cmd
    )

    if not pretend:
        job_id = run_cmd('qsub', inp=pbs)
        return job_id.strip()
    else:
        return cmd


def submit_jobs(commands, job_log, is_pretend, **kwargs):
    job_template = get_job_template()

    for i, cmd in enumerate(commands):
        prefix = '' if len(commands) == 1 else ('%d: ' % i)

        job_id = submit(cmd, job_template=job_template, **kwargs)
        print(prefix + job_id)

        if job_log:
            job_log.write('[%s]\t%s\t"%s"\n' % (datetime.now().isoformat(), job_id, cmd))

        if not is_pretend:  # we're just printing commands, do it as fast as possible
            time.sleep(0.1)


def sanitize_cmd(bit):
    """ Sanitize a submitted command, add quotations etc...

    :param bit: A part of command to sanitize
    :type bit: str
    :return: Sanitized part of command
    :rtype: str
    """

    if "'" in bit and not re.search("^($|'|\")", bit):
        return '"%s"' % (bit,)
    elif re.search(r"[${[\]!} ]", bit) and "'" not in bit:
        return "'%s'" % (bit,)
    elif bit == "awkt":
        return "awk -F '\t' -v OFS='\t'"
    elif bit == 'sortt':
        return "sort -t $'\t'"

    return bit
