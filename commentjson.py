# future statements
# no statements

# default modules
import json

# installed modules
# no modules

# project modules
# no modules

def __load_from_lines(lines, **kwargs):
    cleaned_lines = []

    for ln in lines:
        ln = ln.split('//')[0].strip()
        if ln != '':
            # remove escaping
            ln = ln.replace('\\/', '/')
            cleaned_lines.append(ln)

    parsed = json.loads('\n'.join(cleaned_lines))
    return parsed


def load(file_obj, **kwargs):
    try:
        return __load_from_lines(file_obj, **kwargs)
    except Exception as e:
        e.args = (
            'error while parsing {} : "{}"'.format(file_obj.name, e.args[0]),
        )
        raise e


def loads(text, **kwargs):
    lines = text.split('\n')
    return __load_from_lines(lines, **kwargs)


def dump(*args, **kwargs):
    return json.dump(*args, **kwargs)


def dumps(*args, **kwargs):
    return json.dumps(*args, **kwargs)
