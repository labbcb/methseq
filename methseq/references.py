from os import walk
from os.path import join, exists, abspath

from methseq import search_regex

FASTA_REGEX = '\\.(fa|fasta)$'


def list_dir(directory):
    files = []
    for dir_path, _, filenames in walk(directory):
        for f in filenames:
            files.append(abspath(join(dir_path, f)))

    return files


def collect_reference_files(directory):
    genome_files = search_regex(directory, FASTA_REGEX)

    if not genome_files:
        raise Exception('No genome FASTA files in found in' + directory)

    bismark_dir = join(directory, "Bisulfite_Genome")
    if not exists(bismark_dir):
        raise Exception('No Bisulfite_Genome directory found in ' + directory)

    index_files_ct = list_dir(join(bismark_dir, 'CT_conversion'))
    index_files_ga = list_dir(join(bismark_dir, 'GA_conversion'))

    return genome_files, index_files_ct, index_files_ga
