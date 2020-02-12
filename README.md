# MethSeq - Automation tool for high-throughput sequencing DNA methylation data

Install **MethSeq** command-line tool

```bash
pip3 install --force --user git+https://github.com/labbcb/methseq.git
```

Supported sequencing protocols for identification of DNA methylation

- WGBS
- Pico-Methyl
- EMSeq

> Reduced-representation bisulfite sequencing (RRBS) data is not supported!

Required software

- Python 3
- Cromwell workflow management system running in server mode
- Docker (optional but highly recommended to improve reproducibility)

Required data

- Paired-end raw sequencing FASTQ files (one file per strand)
- Indexed reference genome using Bismark 0.22.2 and Bowtie 2.3.5.1

Used software via Docker container images

- TrimGalore version 0.6.5
- Bismark 0.22.2
- Bowtie 2.3.5.1
- FastQC 

> Your compute environment may not support Docker. Install those software locally and overwrite their paths using **MethSeq** command-line parameters.

Main data processing tasks

- Trim and filter raw sequencing reads (TrimGalore!)
- Align filtered reads to methylation-aware indexed reference genome (Bismark, Bowtie 2)
- Deduplicate aligned reads to remove PCR-bias (Bismark)
- Extract DNA methylation of CpG context genome-wide (Bismark)

Quality control and report tasks

- Quality assessment of filtered sequencing reads (FastQC)
- Overall report (Bismark)

Expected result files for each sample, independently of sequencing protocol

- **cpg_report** Genome-wide DNA methylation in CpG context, contains strand information
- **cov** Coverage file, doesn't contain strand information (only CpG with coverage)
- **bedgraph** BedGraph file
- **mbias_png_1** M-Bias plot of forward strand (R1)
- **mbias_png_2** M-Bias Plot of reverse strand (R2, except PicoMethyl)
- **trim_stats_1** Trimming statistics of forward strand (R1)
- **trim_stats_2** Trimming statistics of reverse strand (R2)
- **align_stats** Alignment stats
- **nucleotide_coverage** 
- **deduplicate_stats** Deduplication statistics
- **mbias_stats** M-Bias statistics (used to generate M-Bias plots)
- **splitting_stats** 
- **trim_qc_report_1** QC report of filtered reads of forward strand (R1)
- **trim_qc_report_zip_1** Zipped file containing QC statistics forward strand (R1)
- **trim_qc_report_2** QC report of filtered reads of reverse strand (R2)
- **trim_qc_report_zip_2** Zipped file containing QC statistics reverse strand (R2)
- **unmapped_qc_report_1** QC report of unmapped reads of forward strand (R1)
- **unmapped_qc_report_zip_1** Zipped file containing QC statistics forward strand (R1)
- **unmapped_qc_report_2** QC report of unmapped reads of reverse strand (R2, except PicoMethyl)
- **unmapped_qc_report_zip_2** Zipped file containing QC statistics reverse strand (R2, except PicoMethyl)
- **report** Overall report

How to use

```bash
methseq accel|pico|emseq --fastq /path/to/fastqs [--fastq /path/to/other_fastqs] [trimming parameters] result_dir
```

By default, trimming step will only trim (Illumina) adapter sequences.
Use the following trimming parameters to cut sequences after adapter removal.
It is useful to remove methylation bias saw in M-Bias plot.

- `--five_prime_clip_R1 N` remove `N` bases from the beginning (5') of forward reads (R1)
- `--three_prime_clip_R1 N` remove `N` bases from the end (3') of forward reads (R1)
- `--five_prime_clip_R1 N` remove `N` bases from the beginning (5') of reverse reads (R2)
- `--three_prime_clip_R1 N` remove `N` bases from the end (3') of reverse reads (R2)

By default, trimming step will trim bases at the end (3') that have PHREAD score lower than 20.
Use `--quality N` to change quality cutoff to `N`.

By default, trimming step will filter trimmed reads that are smaller than 20 bp.
Use `--length N` to change read length cutoff to `N`. 

**MethSeq** will

1. Check if all samples are paired FASTQ files
2. Check if indexed reference genome files exists
3. Generate inputs JSON file
4. Write workflow (WDL) and JSON files to result directory
5. Submit WDL and JSON files to Cromwell server through its API
6. Wait until workflow execution is completed
7. If success, collect workflow output files copping (or moving) them to result directory 

> Extra: run MultiQC on `result_dir` folder!

Use cases

To process WGBS samples with trimming parameters to remove methylation bias saw in M-Bias plot.

```bash
methseq wgbs \
    --fastq /path/to/wgbs_fastqs \
    --five_prime_clip_R1 16 \
    --three_prime_clip_R1 16 \
    --five_prime_clip_R2 16 \
    --three_prime_clip_R2 16 \
    /path/to/wgbs_result
```

To process a single sample (EMSeq) passing path to specific file.
It is useful to run **methseq** on files that are in the same directory with other files.

```bash
methseq emseq \
    --fastq_1 /path/to/emseq_1.fastq.gz \
    --fastq_2 /path/to/emseq_2.fastq.gz \
    /path/to/wgbs_result
```

## Development

Clone git repository from GitHub.

```bash
git clone https://github.com/labbcb/methseq.git
cd methseq
```

Create virtual environment and install development version.

```bash
python3 -m venv venv
source venv/bin/activate
pip install --requirement requirements.txt
```

Publish new methseq version to Pypi.

```bash
pip install setuptools wheel twine
python setup.py sdist bdist_wheel
twine upload -u $PYPI_USER -p $PYPI_PASS dist/*
```