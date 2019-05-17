package com.contactsunny.poc.sparkReadAndWriteFromS3POC.utils;

import com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain.FileInputLine;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain.FinalResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static com.contactsunny.poc.sparkReadAndWriteFromS3POC.config.CustomConstants.URL_DELIMITER;

public class FileUtil {

    private S3Util s3Util;
    private SparkSession sparkSession;

    public FileUtil(
            S3Util _s3Config,
            SparkSession _sparkSession) {

        this.s3Util = _s3Config;
        this.sparkSession = _sparkSession;

    }

    public Dataset<FileInputLine> getDatasetFromS3(String filePath) {

        Dataset<FileInputLine> fileDataSet = null;

        String s3FilePath = this.s3Util.getS3FileURLFromPath(filePath);

        this.s3Util.setHadoopS3Configuration();

        fileDataSet = this.sparkSession.read().option("header", "true").csv(s3FilePath)
                .as(Encoders.bean(FileInputLine.class));

        return fileDataSet;
    }

    public void writeDatasetToS3File(Dataset<FinalResult> dataset, String filePath) {

        String s3FilePath = this.s3Util.getS3FileURLFromPath(filePath) + URL_DELIMITER;

        dataset
            .write()
            .option("header", "true")
            .mode(SaveMode.Append)
            .csv(s3FilePath);
    }
}
