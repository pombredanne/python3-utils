
def fmap(fn_list, arg):
    """Iteratively applies the functions in fn_list to argument arg"""
    for fn in fn_list:
        arg = fn(arg)
    return arg
