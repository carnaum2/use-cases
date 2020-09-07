
from os import listdir
from os.path import isfile, isdir, join
def item_line_count(path):
    if isdir(path):
        return dir_line_count(path)
    elif isfile(path):
        return len(open(path, 'rb').readlines())
    else:
        return 0
def dir_line_count(dir):
    return sum(map(lambda item: item_line_count(join(dir, item)), listdir(dir)))