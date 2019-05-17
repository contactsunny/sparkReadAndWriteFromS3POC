package com.contactsunny.poc.sparkReadAndWriteFromS3POC.config;

public class CustomConstants {

    public static final String URL_DELIMITER = "/";
    public static final String HDP_FS_S3_IMPL_KEY = "fs.s3a.impl";
    public static final String HDP_FS_S3_IMPL_VALUE = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    public static final String HDP_FS_S3_AWS_ACCESS_KEY_KEY = "fs.s3a.awsAccessKeyId";
    public static final String HDP_FS_S3_AWS_ACCESS_SECRET_KEY = "fs.s3a.awsSecretAccessKey";
    public static final String COLUMN_DOUBLE_UDF_NAME = "columnDoubleUdf";
    public static final String DOUBLED_COLUMN_NAME = "numberDoubled";
    public static final String NUMBER_COLUMN_NAME = "number";
}
