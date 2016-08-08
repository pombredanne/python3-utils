import os
import sys
import subprocess


def mkdir_p(path):
    """ from http://stackoverflow.com/questions/600268"""
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def process_call(cmd, shell=False, ignore_err=False):
    """Execute proces call specified in list cmd"""
    if shell:
        cmd = ' '.join(cmd)

    proc = subprocess.Popen(
        cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    msg_out, msg_err = proc.communicate()

    msg_err = '\n'.join([e for e in msg_err.split('\n')
                         if e.find('loading') < 0])
    if not ignore_err:
        if msg_err:
            err_msg = ('subprocess exited with error: \n%s' %
                       '\n'.join([(' ' * 4 + str(l))
                                  for l in msg_err.split('\n')]))
            raise IOError(err_msg)
        else:
            return msg_out
    else:
        return msg_out, msg_err
