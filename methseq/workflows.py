from os.path import join, basename
from zipfile import ZipFile

from pkg_resources import resource_filename

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
