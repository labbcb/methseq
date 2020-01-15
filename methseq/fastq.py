import gzip
from os.path import basename
from re import search, IGNORECASE

from methseq import search_regex

FASTQ_NAME_REGEX = '(?P<sample>.+)_R?[12]\\.fastq(\\.gz)?$'

def extract_sample_name(file, regex):
    """
    Extract sample name from FASTQ or VCF file name
    :param file: a single FASTQ or VCF file
    :param regex: regex pattern used to extract sample name
    :return: sample name
    """
    filename = basename(file)
    result = search(regex, filename, IGNORECASE)

    if not result:
        raise Exception('Unable to extract sample name from ' + filename)

    return result.group('sample')


def collect_fastq_files(directory):
    """
    Search for paired-end FASTQ files and check parity
    :param directory: Directory containing paired-end FASTQ files
    :return: two lists with paths to FASTQ files (forward, reverse)
    """
    forward_files = search_regex(directory, '_R?1\\.fastq(\\.gz)?')
    reverse_files = search_regex(directory, '_R?2\\.fastq(\\.gz)?')

    forward_len = len(forward_files)
    reverse_len = len(reverse_files)

    if forward_len == 0 or reverse_len == 0:
        raise Exception('FASTQ files not found in {}'.format(directory))

    if forward_len != reverse_len:
        raise Exception('FASTQ files not even. Forward: {}, Reverse: {}'.format(forward_len, reverse_len))

    forward_files.sort()
    reverse_files.sort()

    sample_names = [extract_sample_name(f, FASTQ_NAME_REGEX) for f in forward_files]
    return forward_files, reverse_files, sample_names


def extract_platform_unit(fastq_file):
    """
    Extract platform unit from FASTQ header
    :param fastq_file: a single FASTQ file
    :return: list of str
    """
    if fastq_file[-3:] == '.gz':
        file = gzip.open(fastq_file, 'rt')
    else:
        file = open(fastq_file)
    try:
        header = file.readline().strip()
        parts = header.split(':')
        return '{}.{}.{}'.format(parts[2], parts[3], parts[9])
    finally:
        file.close()


def extract_platform_units(fastq_files):
    return [extract_platform_unit(f) for f in fastq_files]