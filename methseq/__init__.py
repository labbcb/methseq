import re
from os import listdir
from os.path import join, basename

name = 'methseq'


def search_regex(directory, regex):
    """
    Search files by regex in directory
    :param directory: list of file path
    :param regex: regex to search
    :return: list of files that match regex
    """
    files = listdir(directory)
    m = re.compile(regex)
    return [join(directory, file) for file in files if m.search(basename(file))]
