from os import listdir
from os.path import join, exists

from methseq import search_regex

FASTA_REREX = '\\.(fa|fasta)$'


def collect_reference_files(directory):
    genome_files = search_regex(directory, FASTA_REREX)

    if not genome_files:
        raise Exception('No genome FASTA files in found in' + directory)

    bismark_dir = join(directory, "Bisulfite_Genome")
    if not exists(bismark_dir):
        raise Exception('No Bisulfite_Genome directory found in ' + directory)

    index_files_ct = listdir(join(bismark_dir, 'CT_conversion'))
    index_files_ga = listdir(join(bismark_dir, 'GA_conversion'))

    return genome_files, index_files_ct, index_files_ga

