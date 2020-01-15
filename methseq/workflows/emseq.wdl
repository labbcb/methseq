version 1.0

workflow EMSeq {

    input {
        File fastq_1
        File fastq_2
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

    call Trim {
        input:
            fastq_1 = fastq_1,
            fastq_2 = fastq_2,
            five_prime_clip_1 = five_prime_clip_1,
            three_prime_clip_1 = three_prime_clip_1,
            five_prime_clip_2 = five_prime_clip_2,
            three_prime_clip_2 = three_prime_clip_2,
            quality = quality,
            trimgalore_path_override = trimgalore_path_override
    }

    call QC as TrimQC_1 {
        input:
            file = Trim.trim_1,
            fastqc_path_override = fastqc_path_override
    }

    call QC as TrimQC_2 {
        input:
            file = Trim.trim_2,
            fastqc_path_override = fastqc_path_override
    }

    call Align {
        input:
            fastq_1 = Trim.trim_1,
            fastq_2 = Trim.trim_2,
            genome_files = genome_files,
            index_files_ct = index_files_ct,
            index_files_ga = index_files_ga,
            bismark_path_override = bismark_path_override,
            bowtie2_path_override = bowtie2_path_override
    }

    call QC as UnmappedQC_1 {
        input:
            file = Align.unmapped_1,
            fastqc_path_override = fastqc_path_override
    }

    call QC as UnmappedQC_2 {
        input:
            file = Align.unmapped_2,
            fastqc_path_override = fastqc_path_override
    }

    call Deduplicate {
        input:
            bam = Align.bam,
            bismark_path_override = bismark_path_override
    }

    call Extract {
        input:
            bam = Deduplicate.deduplicated,
            genome_files = genome_files,
            bismark_path_override = bismark_path_override
    }

    call Report {
        input:
            alignment_report = Align.stats,
            nucleotide_report = Align.nucleotide_coverage,
            dedup_report = Deduplicate.stats,
            splitting_report = Extract.splitting_stats,
            mbias_report = Extract.mbias_stats,
            bismark_path_override = bismark_path_override
    }

    output {
        File cpg_report = Extract.cpg_report
        File cov = Extract.cov
        File bedgraph = Extract.bedgraph
        File mbias_png_1 = Extract.mbias_png_1
        File mbias_png_2 = Extract.mbias_png_2

        File trim_stats_1 = Trim.stats_1
        File trim_stats_2 = Trim.stats_2

        File align_stats = Align.stats

        File deduplicate_stats = Deduplicate.stats
        File nucleotide_coverage = Align.nucleotide_coverage

        File mbias_stats = Extract.mbias_stats
        File splitting_stats = Extract.splitting_stats

        File trim_qc_report_1 = TrimQC_1.report
        File trim_qc_report_zip_1 = TrimQC_1.reportZip
        File trim_qc_report_2 = TrimQC_2.report
        File trim_qc_report_zip_2 = TrimQC_2.reportZip
        File unmapped_qc_report_1 = UnmappedQC_1.report
        File unmapped_qc_report_zip_1 = UnmappedQC_1.reportZip
        File unmapped_qc_report_2 = UnmappedQC_2.report
        File unmapped_qc_report_zip_2 = UnmappedQC_2.reportZip

        File report = Report.report
    }
}

task Trim {

    input {
        File fastq_1
        File fastq_2

        Int? five_prime_clip_1
        Int? three_prime_clip_1
        Int? five_prime_clip_2
        Int? three_prime_clip_2

        Int quality = 20

        String cores = "4"
        String? trimgalore_path_override
        String trim_galore_path = select_first([trimgalore_path_override, ""]) + "trim_galore"
    }


    command <<<
        ~{trim_galore_path} \
            --paired \
            --illumina \
            --gzip \
            ~{'--clip_R1 ' + five_prime_clip_1} \
            ~{'--three_prime_clip_R1 ' + three_prime_clip_1} \
            ~{'--clip_R2 ' + five_prime_clip_2} \
            ~{'--three_prime_clip_R2 ' + three_prime_clip_2} \
            --quality ~{quality} \
            --cores ~{cores} \
            ~{fastq_1} \
            ~{fastq_2}
    >>>

    output {
        File trim_1 = basename(fastq_1, ".fastq.gz") + "_val_1.fq.gz"
        File trim_2 = basename(fastq_2, ".fastq.gz") + "_val_2.fq.gz"
        File stats_1 = basename(fastq_1) + "_trimming_report.txt"
        File stats_2 = basename(fastq_2) + "_trimming_report.txt"
    }

    runtime {
        docker: "welliton/trimgalore:0.6.5"
        cpu: cores
        memory: "1 GB"
    }
}

task Align {

    input {
        File fastq_1
        File fastq_2
        Array[File] genome_files
        Array[File] index_files_ct
        Array[File] index_files_ga

        String? bismark_path_override
        String bismark_path = select_first([bismark_path_override, ""]) + "bismark"

        String? bowtie2_path_override
        String cores = "4"
    }

    command <<<
        mkdir genome_folder
        mv ~{sep=' ' genome_files} genome_folder
        mkdir genome_folder/Bisulfite_Genome
        mkdir genome_folder/Bisulfite_Genome/CT_conversion/
        mkdir genome_folder/Bisulfite_Genome/GA_conversion/
        mv ~{sep=' ' index_files_ct} genome_folder/Bisulfite_Genome/CT_conversion/
        mv ~{sep=' ' index_files_ga} genome_folder/Bisulfite_Genome/GA_conversion/

        ~{bismark_path} \
            --genome genome_folder \
            -1 ~{fastq_1} \
            -2 ~{fastq_2} \
            --bowtie2 \
            ~{'--bowtie2_path ' + bowtie2_path_override} \
            --bam \
            --parallel ~{cores} \
            --un \
            --nucleotide_coverage
    >>>

    output {
        File bam = basename(fastq_1, ".fq.gz") + "_bismark_bt2_pe.bam"
        File stats = basename(fastq_1, ".fq.gz") + "_bismark_bt2_PE_report.txt"
        File nucleotide_coverage = basename(fastq_1, ".fq.gz") + "_bismark_bt2_pe.nucleotide_stats.txt"
        File unmapped_1 = basename(fastq_1) + "_unmapped_reads_1.fq.gz"
        File unmapped_2 = basename(fastq_2) + "_unmapped_reads_2.fq.gz"
    }

    runtime {
        docker: "welliton/bismark:0.22.2"
        cpu: "12"
        memory: "40 GB"
    }
}

task Deduplicate {

    input {
        File bam

        String? bismark_path_override
        String deduplicate_bismark_path = select_first([bismark_path_override, ""]) + "deduplicate_bismark"
    }

    command <<<
        ~{deduplicate_bismark_path} \
            --paired \
            --bam \
            ~{bam}
    >>>

    output {
        File deduplicated = basename(bam, ".bam") + ".deduplicated.bam"
        File stats = basename(bam, ".bam") + ".deduplication_report.txt"
    }

    runtime {
        docker: "welliton/bismark:0.22.2"
        cpu: "3"
        memory: "50 GB"
    }
}

task Extract {

    input {
        File bam
        Array[File] genome_files

        String? bismark_path_override
        String bismark_methylation_extractor_path = select_first([bismark_path_override, ""]) + "bismark_methylation_extractor"
        String cores = "4"
    }


    command <<<
        mkdir genome_folder
        mv ~{sep=' ' genome_files} genome_folder

        ~{bismark_methylation_extractor_path} \
            --paired-end \
            --cytosine_report \
            --genome_folder genome_folder \
            --gzip \
            --parallel ~{cores} \
            ~{bam}
    >>>

    output {
        File cpg_report = basename(bam, ".bam") + ".CpG_report.txt.gz"
        File cov = basename(bam, ".bam") + ".bismark.cov.gz"
        File bedgraph = basename(bam, ".bam") + ".bedGraph.gz"
        File mbias_png_1 = basename(bam, ".bam") + ".M-bias_R1.png"
        File mbias_png_2 = basename(bam, ".bam") + ".M-bias_R2.png"
        File mbias_stats = basename(bam, ".bam") + ".M-bias.txt"
        File splitting_stats = basename(bam, ".bam") + "_splitting_report.txt"
    }

    runtime {
        docker: "welliton/bismark:0.22.2"
        cpu: "12"
        memory: "5 GB"
    }
}

task Report {

    input {
        File alignment_report
        File dedup_report
        File splitting_report
        File mbias_report
        File nucleotide_report

        String? bismark_path_override
        String bismark_report_path = select_first([bismark_path_override, ""]) + "bismark2report"
    }

    command <<<
        ~{bismark_report_path} \
            --alignment_report ~{alignment_report} \
            --dedup_report ~{dedup_report} \
            --splitting_report ~{splitting_report} \
            --mbias_report ~{mbias_report} \
            --nucleotide_report ~{nucleotide_report}
    >>>

    output {
        File report = basename(alignment_report, ".txt") + ".html"
    }

    runtime {
        docker: "welliton/bismark:0.22.2"
        cpu: "1"
        memory: "1 GB"
    }
}

task QC {

    input {
        File file

        String? fastqc_path_override
        String fastqc_path = select_first([fastqc_path_override, ""]) + "fastqc"
    }

    command <<<
        ~{fastqc_path} --outdir . ~{file}
    >>>

    output {
        File report = basename(file, ".fq.gz") + "_fastqc.html"
        File reportZip = basename(file, ".fq.gz") + "_fastqc.zip"
    }

    runtime {
        docker: "welliton/fastqc:0.11.8"
        cores: "1"
        memory: "1 GB"
    }
}
