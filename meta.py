import sys
import time
import traceback
from functools import wraps


def ensure_version(major=None, minor=None, micro=None, releaselevel=None):
    """Raise an error if the script is not running on the required version"""
    def wrapper_decorator(method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            ok_version = (
                (major is None or sys.version_info.major >= major) and
                (minor is None or sys.version_info.minor >= minor) and
                (micro is None or sys.version_info.micro >= micro) and
                (
                    releaselevel is None or
                    sys.version_info.releaselevel == releaselevel
                )
            )
            if ok_version:
                return method(*args, **kwargs)

            current_version = '{}.{}.{}-{}'.format(
                sys.version_info.major,
                sys.version_info.minor,
                sys.version_info.micro,
                sys.version_info.releaselevel
            )
            required_version = '{}.{}.{}-{}'.format(
                major if major else 'x',
                minor if minor else 'x',
                micro if micro else 'x',
                releaselevel if releaselevel else 'x'
            )

            msg = (
                'This version of Python is not supported '
                '(current: {}, required: {})'.format(
                    current_version, required_version
                )
            )
            print(msg)
            exit(1)

        return wrapper
    return wrapper_decorator


class StatusPrinter(object):
    def __init__(self, print_every=10000, comment=None, total_cnt=None):
        self.print_every = print_every
        self.cnt = 0
        self.total_cnt = total_cnt
        self.message = ('[status]'
                        ' {}:'.format(comment) if comment else ' ')
        self.start = time.time()

    def flush(self):
        self.cnt = 0
        self.start = time.time()

    def increase(self):
        self.cnt += 1
        if self.cnt % self.print_every == 0:
            delta = time.time() - self.start
            status = (
                '{:.2%} ({:,})'.format(self.cnt / self.total_cnt, self.cnt)
                if self.total_cnt else '{:,}'.format(self.cnt)
            )
            print('{} {} processed in {:.2f} s (avg {:.1e} s)'
                  ''.format(self.message, status, delta, delta / self.cnt))

    def decorate_method(self, method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            iterator = method(*args, **kwargs)
            for elem in iterator:
                yield elem
                self.increase()
        return wrapper

    def wrap_iterator(self, iterator):
        for elem in iterator:
            yield elem
            self.increase()


def timer(func, printer=None, comment=None):
    """Times function func"""
    local_printer = printer if printer is not None else print
    local_comment = comment if comment is not None else func.__name__

    @wraps(func)
    def wrapper(*args, **kwargs):
        printer = kwargs.pop('printer', local_printer)
        comment = kwargs.pop('comment', local_comment)

        start = time.time()
        resp = func(*args, **kwargs)
        elapsed = time.time() - start
        if elapsed > 3600:
            timestr = ('{:02.0f}:{:02.0f}:{:05.2f}'.format(
                    elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60))
        elif elapsed > 60:
            timestr = ('{:02.0f}:{:05.2f}'.format(
                    (elapsed % 3600) // 60, elapsed % 60))
        else:
            timestr = ('{:.3f} s'.format(elapsed))

        printer('[timer] {} : {}'.format(comment, timestr))

        return resp

    return wrapper


def error_wrapper_pool(method):
    """Make sure that your multiprocessing pool workers report
    their full traceback when crashing."""

    @wraps(method)
    def wrapper(*args, **kwargs):
        try:
            resp = method(*args, **kwargs)
        except Exception as exception:
            trace = traceback.format_exception(*sys.exc_info())
            raise exception.__class__(''.join(trace))
        return resp

    return wrapper


def cls_decorate_all(decorator, exclude=None):
    """Decorate all methods in class except those in exclude"""

    if exclude is None:
        exclude = set()

    if not callable(exclude):
        def exclude_func(e): return e in exclude
    else:
        exclude_func = exclude

    def wrapper(cls_):
        for attr in cls_.__dict__:
            mthd = getattr(cls_, attr)
            if callable(mthd) and not exclude_func(attr):
                setattr(cls_, attr, decorator(mthd))
        return cls_

    return wrapper


class Printer(object):
    def __init__(self, global_indent=0, on=True, base_space='  '):
        self.on = on
        self.global_indent = global_indent
        self.base_space = base_space

    def __call__(self, status,
                 output=1, local_indent=0, end='\n', no_indent=False):
        if not self.on:
            return

        if output == 1 or output == 2:
            output = [sys.stdout, sys.stderr][output - 1]
        else:
            raise TypeError('use "1" for stdout, "2" for stderr')

        if no_indent:
            spacing = ''
        else:
            spacing = self.base_space * (self.global_indent + local_indent)

        status = u'{}'.format(status).split('\n')
        for ln in status:
            print(spacing + ln, file=output, end=end)

    def clone(self, increase_indent=False):
        global_indent = self.global_indent + (1 if increase_indent else 0)
        base_space = self.base_space
        on = self.on
        return Printer(global_indent, on, base_space)
