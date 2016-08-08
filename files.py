import datetime

def datetime_filename():
    """Returns a date/time string in a format suitable to be used for
    naming a file."""
    return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

