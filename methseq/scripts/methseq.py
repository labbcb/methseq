from os import mkdir
from os.path import exists, abspath

import click

from ..fastq import collect_fastq_files
from ..references import collect_reference_files
from ..scripts import submit_workflow


@click.command()
@click.option('--host', help='Cromwell server URL')
@click.option('--workflow', type=click.Choice(['wgbs', 'pico', 'emseq']), default='wgbs',
              help='Sequencing protocol for DNA methylation profiling', show_default=True)
@click.option('--fastq', 'fastq_directories', multiple=True, type=click.Path(exists=True),
              help='Path to directory containing paired-end FASTQ files')
@click.option('--fastq_1', 'fastq_1_files', multiple=True, type=click.Path(exists=True),
              help='Path to FASTQ files, forward (R1)')
@click.option('--fastq_2', 'fastq_2_files', multiple=True, type=click.Path(exists=True),
              help='Path to FASTQ files, reverse (R2)')
@click.option('--reference', 'reference_dir', required=True, type=click.Path(exists=True),
              help='Path to directory containing reference files (including Bisulfite_Genome directory)')
@click.option('--five_prime_clip_1', type=click.INT, help="Remove bases from the beginning (5') of forward strand (R1)")
@click.option('--three_prime_clip_1', type=click.INT, help="Remove bases from the end (3') of forward strand (R1)")
@click.option('--five_prime_clip_2', type=click.INT, help="Remove bases from the beginning (5') of reverse strand (R2)")
@click.option('--three_prime_clip_2', type=click.INT, help="Remove bases from the end (3') of reverse strand (R2)")
@click.option('--quality', type=click.INT, default=20,
              help="Remove bases at the end that have low Phread score", show_default=True)
@click.option('--dont_run', is_flag=True, default=False, show_default=True,
              help='Do not submit workflow to Cromwell. Just create destination directory and write JSON and WDL files')
@click.option('--sleep', 'sleep_time', default=300, type=click.INT,
              help='Time to sleep (in seconds) between each workflow status check')
@click.option('--move', is_flag=True, default=False,
              help='Move output files to destination directory instead of copying them')
@click.option('--trimgalore_path_override')
@click.option('--bismark_path_override')
@click.option('--bowtie2_path_override')
@click.option('--fastqc_path_override')
@click.argument('destination', type=click.Path())
def cli(host,
        workflow,
        fastq_directories,
        fastq_1_files,
        fastq_2_files,
        reference_dir,
        five_prime_clip_1,
        three_prime_clip_1,
        five_prime_clip_2,
        three_prime_clip_2,
        quality,
        dont_run,
        sleep_time,
        move,
        trimgalore_path_override,
        bismark_path_override,
        bowtie2_path_override,
        fastqc_path_override,
        destination):
    """
    Automation tool for high-throughput sequencing DNA methylation data
    """

    if not fastq_directories and (not fastq_1_files or not fastq_2_files):
        click.echo("At least one --fastq (directory) or --fastq_1 and --fastq_2 (files) must be provided.", err=True)
        exit(1)

    if not exists(destination):
        mkdir(destination)
    destination = abspath(destination)

    workflow = {'wgbs': 'WGBSBatch', 'pico': 'PicoMethylBatch', 'emseq': 'EMSeqBatch'}[workflow]

    for fastq_dir in fastq_directories:
        forward_files, reverse_files, sample_names = collect_fastq_files(fastq_dir)
        fastq_1_files.extend(forward_files)
        fastq_2_files.extend(reverse_files)

    genome_files, index_files_ct, index_files_ga = collect_reference_files(reference_dir)

    inputs = dict()
    inputs[workflow + ".fastqs_1"] = fastq_1_files
    inputs[workflow + ".fastqs_2"] = fastq_2_files
    inputs[workflow + ".genome_files"] = genome_files
    inputs[workflow + ".index_files_ct"] = index_files_ct
    inputs[workflow + ".index_files_ga"] = index_files_ga

    if five_prime_clip_1 is not None:
        inputs[workflow + ".five_prime_clip_1"] = five_prime_clip_1
    if three_prime_clip_1 is not None:
        inputs[workflow + ".three_prime_clip_1"] = three_prime_clip_1
    if five_prime_clip_2 is not None:
        inputs[workflow + ".five_prime_clip_2"] = five_prime_clip_2
    if three_prime_clip_2 is not None:
        inputs[workflow + ".three_prime_clip_2"] = three_prime_clip_2

    inputs[workflow + ".quality"] = quality

    if trimgalore_path_override is not None:
        inputs[workflow + ".trimgalore_path_override"] = trimgalore_path_override
    if bismark_path_override is not None:
        inputs[workflow + ".bismark_path_override"] = bismark_path_override
    if bowtie2_path_override is not None:
        inputs[workflow + ".bowtie2_path_override"] = bowtie2_path_override
    if fastqc_path_override is not None:
        inputs[workflow + ".fastqc_path_override"] = fastqc_path_override

    submit_workflow(host, workflow, inputs, destination, sleep_time, dont_run, move)
