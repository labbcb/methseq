import shutil
from itertools import chain
from json import dump
from os.path import join, basename, exists
from time import sleep
from zipfile import ZipFile

import click
from pkg_resources import resource_filename

from ..cromwell import CromwellClient

WORKFLOW_FILES = {'WGBS': 'workflows/wgbs.wdl',
                  'WGBSBatch': 'workflows/wgbs-batch.wdl',
                  'PicoMethyl': 'workflows/pico.wdl',
                  'PicoMethylBatch': 'workflows/pico-batch.wdl',
                  'EMSeq': 'workflows/emseq.wdl',
                  'EMSeqBatch': 'workflows/emseq-batch.wdl'}

IMPORTS_FILES = {'WGBSBatch': ['WGBS'],
                 'PicoMethylBatch': ['PicoMethyl'],
                 'EMSeqBatch': ['EMSeq']}

WORKFLOW_INPUT_FILES = {'WGBS': 'wgbs.inputs.json',
                        'WGBSBatch': 'wgbs-batch.inputs.json',
                        'PicoMethyl': 'pico.inputs.json',
                        'PicoMethylBatch': 'pico-batch.inputs.json',
                        'EMSeq': 'emseq.inputs.json',
                        'EMSeqBatch': 'emseq-batch.inputs.json'}


def get_workflow_file(workflow):
    """
    Return package path to workflow file
    :param workflow: Workflow name
    :return: path to workflow file
    """

    if workflow not in WORKFLOW_FILES.keys():
        raise Exception('Workflow not found: ' + workflow)

    return resource_filename(__name__, WORKFLOW_FILES[workflow])


def zip_imports_files(workflow, dest_dir):
    """
    Zip WDL files and write in destination directory
    :param workflow: workflow name
    :param dest_dir: destination directory
    :return: path to zip file or None if workflow does not require sub-workflows
    """

    if workflow not in IMPORTS_FILES.keys():
        return None

    zip_file = join(dest_dir, workflow + '.imports.zip')
    with ZipFile(zip_file, 'w') as file:
        for sub_workflow in IMPORTS_FILES[workflow]:
            workflow_file = get_workflow_file(sub_workflow)
            file.write(workflow_file, basename(workflow_file))

    return zip_file


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
