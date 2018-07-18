import os
import re
import time
from subprocess import Popen, PIPE

HOME = os.getenv("HOME")
CWD = os.getcwd()
PBS_OUTPUT = os.path.join(HOME, 'pbs-output')
PATH = os.getenv('PATH')


def _sanitize_cmd(bit):
    if "'" in bit and not re.match("^($|'|\")", bit):
        return '"%s"' % (bit,)
    elif re.match("[${[\]!} ]", bit) and "'" not in bit:
        return "'%s'" % (bit,)
    elif bit == 'awkt':
        return "awk -F '\t' -v OFS='\t'"
    elif bit == 'sortt':
        return "sort -t $'\t'"

    return bit


def submit(cmd, walltime=24, memory=2, cpu=1, wd=CWD, output_dir=PBS_OUTPUT, path=PATH, job_name=None):
    """Submits a command to the cluster

    :param cmd: The command to run.
    :param walltime: Requested run-time limit in hours. Default 24hrs.
    :param memory: Requested memory limit in GB. Default 2GB.
    :param cpu: Requested number of CPU. Default 1 CPU.
    :param wd: Working directory. Default is cwd().
    :param output_dir: Where to save job output. Default is $HOME/pbs-output
    :param path: Job's PATH. Default is $PATH.
    :param job_name: Name of the job as displayed by qstat. Default is command name, ie: awk
    :type cmd: str
    :type walltime: float
    :type memory: float
    :type cpu: int
    :type wd: str
    :type output_dir: str
    :type path: str
    :type job_name: str
    :return: Job id returned by qsub.
    :rtype: str
    """
    walltime = '%02d:%02d:00' % (walltime, 60 * (walltime % 1))
    memory = '%dM' % (1024 * memory,)
    cpu = '%d' % (cpu,)

    resources = ['walltime=%s' % (walltime,), 'mem=%s' % (memory,), 'nodes=1:ppn=%s' % (cpu,)]
    resources = ','.join(resources)

    cmd_echo = cmd.replace('$', '\$').replace('"', '\"')

    if not job_name:
        job_name = cmd.split()[0]  # Remove anything following a space (can be introduced during smart quoting)
        job_name = os.path.split(job_name)[-1]  # Remove the path before any command
        job_name = job_name.replace('&', '')  # Remove any ampersands
        job_name = re.sub(r'^\d+', '', job_name)  # Remove any leading digits, otherwise qsub will throw an error

    proc = Popen('qsub', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)

    pbs = """#PBS -S /bin/bash
#PBS -e localhost:{pbs_output}
#PBS -o localhost:{pbs_output}
#PBS -j oe
#PBS -l {resources}
#PBS -m a
#PBS -r n
#PBS -V
#PBS -N {name}
cd {cwd}
export PATH='{path}'
export PBS_NCPU={cpu}
echo -E '==> Run command    :' "{cmd_echo}"
echo    '==> Execution host :' `hostname`
{cmd}
""".format(
        pbs_output=output_dir,
        resources=resources,
        name=job_name,
        cwd=wd,
        path=path,
        cpu=cpu,
        cmd_echo=cmd_echo,
        cmd=cmd
    )

    job_id, err = proc.communicate(input=pbs)
    if err:
        raise Exception(err)

    return job_id.strip()


def main():
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(
        description='Submit a job to queue.',
        epilog="Job STDERR is merged with STDOUT and redirected to %s/pbs-output/. "
               "Any job exceeding the run time and memory limits will be killed automatically." % (HOME,))
    parser.add_argument('command', nargs=argparse.REMAINDER,
                        help='The command to run on the cluster. Note that any output redirection or pipe symbols '
                             'must be escaped, i.e.   \\> or \\|')
    parser.add_argument('-w', '-walltime', '--walltime', type=float, default=24,
                        help='The expected run time of the job, measured in hours.')
    parser.add_argument('-m', '-mem', '--mem', type=float, default=2,
                        help='The maximum amount of memory used by the job in Gb.')
    parser.add_argument('-c', '-cpu', '--cpu', type=int, default=1,
                        help='The number of CPUs required on a single node.')
    parser.add_argument('-f', '-file', '--file', type=argparse.FileType('rU'),
                        help='Read commands from a file, one per line. If a "command" is specified as a positional '
                             'argument this will be ignored.')
    parser.add_argument('-l', '-disable-log', '--disable-log', action='store_true',
                        help='Write a log of submitted jobs to "log-path".')
    parser.add_argument('-L', '-log-path', '--log-path', default=os.path.join(HOME, '.pbs_log'),
                        type=argparse.FileType('a'),
                        help='Write a log of submitted jobs.')

    args = parser.parse_args()

    if not args.command and not args.file:
        parser.error("Missing command to submit")

    commands = []

    if args.command:
        # single command takes precedence
        commands.append(' '.join(map(_sanitize_cmd, args.command)))
    elif args.file:
        commands = [c.strip() for c in args.file]

    for i, cmd in enumerate(commands):
        prefix = '' if len(commands) == 1 else ('%d: ' % i)

        job_id = submit(cmd, args.walltime, args.mem, args.cpu)
        print(prefix + job_id)

        if not args.disable_log:
            args.log_path.write('[%s]\t%s\t"%s"\n' % (datetime.now().isoformat(), job_id, cmd))

        time.sleep(0.1)


if __name__ == '__main__':
    main()
