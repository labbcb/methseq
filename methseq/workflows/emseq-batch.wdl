version 1.0

import "emseq.wdl" as EMSeq

workflow EMSeqBatch {

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

        call EMSeq.EMSeq {
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
        Array[File] cpg_report = EMSeq.cpg_report
        Array[File] cov = EMSeq.cov
        Array[File] bedgraph = EMSeq.bedgraph
        Array[File] mbias_png_1 = EMSeq.mbias_png_1
        Array[File] mbias_png_2 = EMSeq.mbias_png_2
        Array[File] trim_stats_1 = EMSeq.trim_stats_1
        Array[File] trim_stats_2 = EMSeq.trim_stats_2
        Array[File] align_stats = EMSeq.align_stats
        Array[File] nucleotide_coverage = EMSeq.nucleotide_coverage
        Array[File] deduplicate_stats = EMSeq.deduplicate_stats
        Array[File] mbias_stats = EMSeq.mbias_stats
        Array[File] splitting_stats = EMSeq.splitting_stats
        Array[File] trim_qc_report_1 = EMSeq.trim_qc_report_1
        Array[File] trim_qc_report_zip_1 = EMSeq.trim_qc_report_zip_1
        Array[File] trim_qc_report_2 = EMSeq.trim_qc_report_2
        Array[File] trim_qc_report_zip_2 = EMSeq.trim_qc_report_zip_2
        Array[File] unmapped_qc_report_1 = EMSeq.unmapped_qc_report_1
        Array[File] unmapped_qc_report_zip_1 = EMSeq.unmapped_qc_report_zip_1
        Array[File] unmapped_qc_report_2 = EMSeq.unmapped_qc_report_2
        Array[File] unmapped_qc_report_zip_2 = EMSeq.unmapped_qc_report_zip_2
        Array[File] report = EMSeq.report
    }
}
