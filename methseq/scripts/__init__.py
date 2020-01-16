import binascii
import gzip
import shutil
from itertools import chain
from json import dump
from os.path import join, basename, exists
from random import sample
from time import sleep

import click

from ..cromwell import CromwellClient
from ..workflows import get_workflow_file, zip_imports_files, WORKFLOW_INPUT_FILES


def is_gzip(filename):
    with open(filename, 'rb') as f:
        return binascii.hexlify(f.read(2)) == b'1f8b'


def count_fastq_reads(file):
    for i, l in enumerate(file):
        pass
    return int((i + 1) / 4)


def subset_paired_fastqs(fastq_1, fastq_2, destination_1, destination_2, percentage=10):
    """
    Subset paired-end (gzipped) FASTQ files given a percentage
    :param fastq_1: file path to FASTQ file, forward (R1)
    :param fastq_2: file path to FASTQ file, forward (R2)
    :param destination_1: file object for sampled FASTQ file (R1) in write and binary modes (wb)
    :param destination_2: file object for sampled FASTQ file (R2) in write and binary modes (wb)
    :param percentage: percentage to sample both FASTQ files
    """
    is_gz_1 = is_gzip(fastq_1)
    if is_gz_1:
        file_1 = gzip.open(fastq_1)
    else:
        file_1 = open(fastq_1)
    num_reads_1 = count_fastq_reads(file_1)
    file_1.close()

    is_gz_2 = is_gzip(fastq_2)
    if is_gz_2:
        file_2 = gzip.open(fastq_2)
    else:
        file_2 = open(fastq_2)
    num_reads_2 = count_fastq_reads(file_2)
    file_2.close()

    if num_reads_1 != num_reads_2:
        raise Exception("Number of reads of R1 ({}) is different of R2 ({}).".format(num_reads_1, num_reads_2))

    num_reads = num_reads_1
    num_subset_reads = int(num_reads * percentage / 100)
    random_reads = sample(range(num_reads), num_subset_reads)
    random_reads.sort()

    if is_gz_1:
        file_1 = gzip.open(fastq_1)
    else:
        file_1 = open(fastq_1)
    if is_gz_2:
        file_2 = gzip.open(fastq_2)
    else:
        file_2 = open(fastq_2)

    next_read = 0
    for read_count in range(num_reads):
        if read_count == random_reads[next_read]:
            destination_1.write(file_1.readline())
            destination_1.write(file_1.readline())
            destination_1.write(file_1.readline())
            destination_1.write(file_1.readline())

            destination_2.write(file_2.readline())
            destination_2.write(file_2.readline())
            destination_2.write(file_2.readline())
            destination_2.write(file_2.readline())

            next_read = next_read + 1
            if next_read == num_subset_reads:
                break
        else:
            file_1.readline()
            file_1.readline()
            file_1.readline()
            file_1.readline()
            file_2.readline()
            file_2.readline()
            file_2.readline()
            file_2.readline()
    file_1.close()
    file_2.close()


def submit_workflow(host, workflow, inputs, destination, sleep_time=300, dont_run=False, move=False):
    """
    Copy workflow file into destination; write inputs JSON file into destination;
    submit workflow to Cromwell server; wait to complete; and copy output files to destination
    :param host: Cromwell server URL
    :param workflow: workflow name
    :param inputs: dict containing inputs data
    :param destination: directory to write all files
    :param sleep_time: time in seconds to sleep between workflow status check
    :param dont_run: Do not submit workflow to Cromwell. Just create destination directory and write JSON and WDL files
    :param move: Move output files to destination directory instead of copying them.
    """

    pkg_workflow_file = get_workflow_file(workflow)
    workflow_file = join(destination, basename(pkg_workflow_file))
    shutil.copyfile(pkg_workflow_file, workflow_file)

    click.echo('Workflow file: ' + workflow_file, err=True)

    imports_file = zip_imports_files(workflow, destination)
    if imports_file:
        click.echo('Workflow imports file: ' + imports_file)

    inputs_file = join(destination, WORKFLOW_INPUT_FILES[workflow])
    with open(inputs_file, 'w') as file:
        dump(inputs, file, indent=4, sort_keys=True)
    click.echo('Inputs JSON file: ' + inputs_file, err=True)

    if dont_run:
        click.echo('Workflow will not be submitted to Cromwell. See workflow files in ' + destination)
        exit()

    if not host:
        host = 'http://localhost:8000'
    client = CromwellClient(host)
    workflow_id = client.submit(workflow_file, inputs_file, dependencies=imports_file)

    click.echo('Workflow submitted to Cromwell Server ({})'.format(host), err=True)
    click.echo('Workflow id: ' + workflow_id, err=True)
    click.echo('Starting {} workflow.. Ctrl-C to abort.'.format(workflow), err=True)

    try:
        while True:
            sleep(sleep_time)
            status = client.status(workflow_id)
            if status != 'Submitted' and status != 'Running':
                click.echo('Workflow terminated: ' + status, err=True)
                break
        if status != 'Succeeded':
            exit(1)
    except KeyboardInterrupt:
        click.echo('Aborting workflow.')
        client.abort(workflow_id)
        exit(1)

    outputs = client.outputs(workflow_id)
    for output in outputs.values():
        if isinstance(output, str):
            files = [output]
        elif any(isinstance(i, list) for i in output):
            files = list(chain.from_iterable(output))
        else:
            files = output

        for file in files:
            if exists(file):
                destination_file = join(destination, basename(file))
                click.echo('Collecting file ' + file, err=True)
                if move:
                    shutil.move(file, destination_file)
                else:
                    shutil.copyfile(file, destination_file)
            else:
                click.echo('File not found: ' + file, err=True)
