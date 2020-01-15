version 1.0

import "pico.wdl" as PicoMethyl

workflow PicoMethylBatch {

    input {
        Array[File] fastqs_1
        Array[File] fastqs_2
        Array[File] genome_files
        Array[File] index_files_ct
        Array[File] index_files_ga

        Int? five_prime_clip_1
        Int? three_prime_clip_1
        Int? five_prime_clip_2
        Int? three_prime_clip_2
        Int quality = 20

        String? trimgalore_path_override
        String? bismark_path_override
        String? bowtie2_path_override
        String? fastqc_path_override
    }

    scatter (idx in range(length(fastqs_1))) {

        call PicoMethyl.PicoMethyl {
            input:
                fastq_1 = fastqs_1[idx],
                fastq_2 = fastqs_2[idx],
                five_prime_clip_1 = five_prime_clip_1,
                three_prime_clip_1 = three_prime_clip_1,
                five_prime_clip_2 = five_prime_clip_2,
                three_prime_clip_2 = three_prime_clip_2,
                quality = quality,
                genome_files = genome_files,
                index_files_ct = index_files_ct,
                index_files_ga = index_files_ga,
                trimgalore_path_override = trimgalore_path_override,
                bismark_path_override = bismark_path_override,
                bowtie2_path_override = bowtie2_path_override,
                fastqc_path_override = fastqc_path_override
        }
    }

    output {
        Array[File] cpg_report = PicoMethyl.cpg_report
        Array[File] cov = PicoMethyl.cov
        Array[File] bedgraph = PicoMethyl.bedgraph
        Array[File] mbias_png_1 = PicoMethyl.mbias_png_1
        Array[File] trim_stats_1 = PicoMethyl.trim_stats_1
        Array[File] trim_stats_2 = PicoMethyl.trim_stats_2
        Array[File] align_stats = PicoMethyl.align_stats
        Array[File] nucleotide_coverage = PicoMethyl.nucleotide_coverage
        Array[File] deduplicate_stats = PicoMethyl.deduplicate_stats
        Array[File] mbias_stats = PicoMethyl.mbias_stats
        Array[File] splitting_stats = PicoMethyl.splitting_stats
        Array[File] trim_qc_report_1 = PicoMethyl.trim_qc_report_1
        Array[File] trim_qc_report_zip_1 = PicoMethyl.trim_qc_report_zip_1
        Array[File] trim_qc_report_2 = PicoMethyl.trim_qc_report_2
        Array[File] trim_qc_report_zip_2 = PicoMethyl.trim_qc_report_zip_2
        Array[File] unmapped_qc_report_1 = PicoMethyl.unmapped_qc_report_1
        Array[File] unmapped_qc_report_zip_1 = PicoMethyl.unmapped_qc_report_zip_1
        Array[File] report = PicoMethyl.report
    }
}
