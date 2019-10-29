#!/usr/bin/env python
import os
import re
import sys
import time
from subprocess import Popen, PIPE

HOME = os.getenv("HOME")
USER = os.getenv("USER")
PATH = os.getenv('PATH')
CWD = os.getcwd()
PBS_OUTPUT = os.path.join(HOME, 'pbs-output')


class SubmitException(Exception):
    pass


def num_jobs_atm():
    """ Count number of jobs for current user in all queues

    :return: Number of jobs
    :rtype: int
    """
    proc = Popen("/usr/bin/qstat -u {0} | grep {0} | wc -l".format(USER), shell=True, stdin=PIPE, stdout=PIPE,
                 stderr=PIPE, close_fds=True, universal_newlines=True)

    ret, err = proc.communicate()
    if err:
        raise SubmitException(err)

    if ret:
        return int(ret)
    return 0


def environment_exists(env_name):
    """Checks if conda environment exists. Throws SubmitException if conda is not available.

    :param env_name: Conda environment name
    :type env_name: str
    :return: Environment exists
    :rtype: bool
    """

    proc = Popen('conda env list', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True,
                 universal_newlines=True)

    ret, err = proc.communicate()
    if err:
        raise SubmitException(err)

    environments = set()
    for line in ret.splitlines():
        if line and not line.startswith('#'):
            environments.add(line.split()[0])

    return env_name in environments


def batch(iterable, n=1):
    """ Adapted from https://stackoverflow.com/a/8290508

    :param iterable: iterable we want to split
    :param n: size of a batch
    :type iterable: [list|tuple]
    :type n: int
    :return: Input interable split into batches
    :rtype: [list|tuple]
    """

    size = len(iterable)
    for ndx in range(0, size, n):
        yield iterable[ndx:min(ndx + n, size)]


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


def submit(cmd, walltime=24, mem=2, cpu=1, email=None, wd=CWD, output_dir=PBS_OUTPUT, path=PATH, job_name=None,
           pretend=False, environment=None):
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

    resources = ['walltime=%s' % (walltime_str,), 'mem=%s' % (memory,), 'nodes=1:ppn=%d' % (cpu,)]
    resources = ','.join(resources)

    cmd_echo = cmd.replace('$', r'\$').replace('"', r'\"')

    if not job_name:
        job_name = cmd.split()[0]  # Remove anything following a space (can be introduced during smart quoting)
        job_name = os.path.split(job_name)[-1]  # Remove the path before any command
        job_name = job_name.replace('&', '')  # Remove any ampersands
        job_name = re.sub(r'^\d+', '', job_name)  # Remove any leading digits, otherwise qsub will throw an error

    job_setup = ''
    if environment:
        job_setup = """source /etc/profile.d/conda.sh
conda activate %s""" % environment

    exposed_config = [
        ('walltime', walltime),
        ('mem', mem),
        ('cpu', cpu),
        ('name', job_name),
        ('conda_environment', environment)
    ]
    # this is a more human readable format than json
    job_config = ','.join("%s=%r" % item for item in exposed_config if item[1])

    if not pretend:
        proc = Popen('qsub', shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True, universal_newlines=True)

        pbs = """#PBS -S /bin/bash
#PBS -e localhost:{pbs_output}
#PBS -o localhost:{pbs_output}
#PBS -j oe
#PBS -l {resources}
#PBS -m {send_email}
#PBS -M {email}
#PBS -r n
#PBS -V
#PBS -N {name}
cd {cwd}
export PATH='{path}'
export PBS_NCPU={cpu}
echo -E '==> Run command    :' "{cmd_echo}"
echo    '==> Execution host :' `hostname`
echo    '==> Job config     :' "{job_config}"

{job_setup}

{cmd}
    """.format(
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

        job_id, err = proc.communicate(input=pbs)
        if err:
            raise Exception(err)

        return job_id.strip()
    else:
        return cmd


def main():
    # noinspection PyCompatibility
    import argparse
    from datetime import datetime

    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        """
        We'd like to have the REMAINDER argument formatted in a nicer way and the epilogue is already formatted
        """

        # noinspection PyProtectedMember
        def _format_args(self, action, default_metavar):
            if action.nargs == argparse.REMAINDER:
                get_metavar = self._metavar_formatter(action, default_metavar)
                return '%s' % get_metavar(1)
            return super(CustomHelpFormatter, self)._format_args(action, default_metavar)

    parser = argparse.ArgumentParser(
        description='Submit a job to queue.',
        formatter_class=CustomHelpFormatter,
        epilog="""Job STDERR is merged with STDOUT and redirected to %s/pbs-output/.
Any job exceeding the run time and memory limits will be killed automatically.

All options following your command are considered a property of your command, please state the
submitjob options like walltime, cpu and memory before the command ie:

EXAMPLE #1: submitjob -w 12 -m 5 my_command.py
\t- walltime is 12 hours
\t- memory is 5GB

EXAMPLE #2: submitjob my_command.py -w 12 -m 5
\t- walltime is 24 hours (default)
\t- memory is 2GB (default) 
\t- -w and -m options are ignored by submitjob and used by my_command.py""" % (HOME,))

    parser.add_argument('command', nargs=argparse.REMAINDER, metavar='CMD OPTIONS INPUT',
                        help='The command to run on the cluster. Note that any output redirection or pipe symbols '
                             'must be escaped, i.e.   \\> or \\|')
    parser.add_argument('-w', '-walltime', '--walltime', type=float, default=24,
                        help='The expected run time in hours, default 24.')
    parser.add_argument('-m', '-mem', '--mem', type=float, default=2,
                        help='Max amount of memory to be used in Gb, default 2.')
    # noinspection PyTypeChecker
    parser.add_argument('-c', '-cpu', '--cpu', type=int, default=1,
                        help='Number of CPUs required on a single node, default 1.')
    parser.add_argument('-e', '-conda-environment', '--conda-environment',
                        help='Activate this environment before running a job.')
    parser.add_argument('-f', '-file', '--file', type=argparse.FileType('rU'),
                        help='Read commands from a file, one per line. If a "command" is specified as a positional '
                             'argument this will be ignored.')
    parser.add_argument('-l', '-disable-log', '--disable-log', action='store_true',
                        help='Disable job logging.')
    parser.add_argument('-L', '-log-path', '--log-path', default=os.path.join(HOME, '.pbs_log'),
                        type=argparse.FileType('a'),
                        help='Where to log submitted jobs.')
    parser.add_argument('-E', '-email', '--email', default=None,
                        help='Send an email to this address when a job ends or is aborted')
    parser.add_argument('-n', '-name', '--name', default=None,
                        help='Give submitted job(s) a verbose name.')
    parser.add_argument('-a', '-args', '--args', type=argparse.FileType('rU'),
                        help='File with a list of arguments to the job for batch submitting. '
                             'Works only with direct command, not -f. '
                             'Batch-size of arguments are appended to the end of command, '
                             'or they replace a "{}" if found.')
    # noinspection PyTypeChecker
    parser.add_argument('-b', '-batch-size', '--batch-size', type=int,
                        help='Number of arguments from <args> to use per job.')
    parser.add_argument('-p', '-pretend', '--pretend', action='store_true',
                        help='Don\'t submit, print the commands out instead.')

    args = parser.parse_args()

    # VARIOUS CHECKS

    if not args.command and not args.file:
        parser.error("Missing command to submit")

    if args.command and re.search(r'^\d', args.command[0]):
        parser.error(
            "You are trying to use the obsolete syntax for submitjob. Please run it with --help to see the new usage.")

    if args.args:
        if args.file:
            parser.error("Arguments (-a) only work with single command, not -f.")
        if not args.batch_size:
            parser.error(
                "Trying to use arguments without batch size. "
                "Please add -b to define how many arguments should be added to command per submitted job.")

    try:
        if args.conda_environment and not environment_exists(args.conda_environment):
            parser.error(
                'Could not find conda environment "%s", '
                'please check spelling and make sure the environment exists' % args.conda_environment)
    except SubmitException:
        parser.error('Conda environments are not supported on this system')

    # PREP WORK

    if args.pretend:  # Don't write log if we don't submit
        args.disable_log = True

    commands = []

    if args.command:
        # single command takes precedence
        if args.file:
            sys.stderr.write("WARNING: Ignoring commands from file (-f/--file), direct command takes precedence.\n")

        if args.args:
            cmd = args.command

            if '{}' not in cmd:
                cmd.append('{}')  # add the args placeholder to the end for appending

            cmd_args = [l.strip() for l in args.args]

            for arg_batch in batch(cmd_args, args.batch_size):
                insert_idx = cmd.index('{}')
                expanded_cmd = cmd[:insert_idx] + [('"%s"' % b) for b in arg_batch] + cmd[insert_idx + 1:]
                commands.append(' '.join(map(sanitize_cmd, expanded_cmd)))
        else:
            commands.append(' '.join(map(sanitize_cmd, args.command)))
    elif args.file:
        # commands from file should be formatted correctly already
        commands = [c.strip() for c in args.file if c.strip()]

    if args.email and len(commands) > 10:
        parser.error("Sending email is not supported when submitting more than 10 jobs in a batch")

    for i, cmd in enumerate(commands):
        prefix = '' if len(commands) == 1 else ('%d: ' % i)

        job_id = submit(cmd, args.walltime, args.mem, args.cpu, args.email, job_name=args.name, pretend=args.pretend,
                        environment=args.conda_environment)
        print(prefix + job_id)

        if not args.disable_log:
            args.log_path.write('[%s]\t%s\t"%s"\n' % (datetime.now().isoformat(), job_id, cmd))

        if not args.pretend:  # we're just printing commands, do it as fast as possible
            time.sleep(0.1)


if __name__ == '__main__':
    main()
