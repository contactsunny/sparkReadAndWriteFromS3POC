package com.contactsunny.poc.sparkReadAndWriteFromS3POC.utils;

import com.contactsunny.poc.sparkReadAndWriteFromS3POC.config.S3Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import static com.contactsunny.poc.sparkReadAndWriteFromS3POC.config.CustomConstants.*;

public class S3Util {

    private S3Config s3Config;
    private JavaSparkContext javaSparkContext;

    public S3Util(S3Config _s3Config, JavaSparkContext _javaSparkContext) {
        this.s3Config = _s3Config;
        this.javaSparkContext = _javaSparkContext;
    }

    String getS3FileURLFromPath(String filePath) {

        return this.s3Config.getProtocol() + this.s3Config.getBucketName() + URL_DELIMITER + filePath;
    }

    void setHadoopS3Configuration() {

        Configuration hadoopConfig = this.javaSparkContext.hadoopConfiguration();
        hadoopConfig.set(HDP_FS_S3_IMPL_KEY, HDP_FS_S3_IMPL_VALUE);
        hadoopConfig.set(HDP_FS_S3_AWS_ACCESS_KEY_KEY, s3Config.getAccessKey());
        hadoopConfig.set(HDP_FS_S3_AWS_ACCESS_SECRET_KEY, s3Config.getAccessSecret());
    }
}
