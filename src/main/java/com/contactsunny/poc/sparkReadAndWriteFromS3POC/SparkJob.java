package com.contactsunny.poc.sparkReadAndWriteFromS3POC;

import com.contactsunny.poc.sparkReadAndWriteFromS3POC.config.S3Config;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain.FileInputLine;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain.FinalResult;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.exceptions.ValidationException;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.utils.FileUtil;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.utils.S3Util;
import com.contactsunny.poc.sparkReadAndWriteFromS3POC.utils.UDFUtil;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.contactsunny.poc.sparkReadAndWriteFromS3POC.config.CustomConstants.*;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

class SparkJob {

    private final Logger logger = Logger.getLogger(App.class);

    private String[] args;

    private String inputFilePath, outputFolderPath, sparkMaster;

    private UDFUtil udfUtil;
    private S3Config s3Config;
    private S3Util s3Util;
    private FileUtil fileUtil;

    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private SparkSession sparkSession;

    SparkJob(String[] args) {
        this.args = args;
    }

    void startJob() throws ValidationException, IOException {

        validateArguments();

        Properties properties = new Properties();
        String propFileName = "application.properties";

        InputStream inputStream = App.class.getClassLoader().getResourceAsStream(propFileName);

        try {
            properties.load(inputStream);
        } catch (IOException e) {
            logger.error(e.getMessage());
            throw e;
        }

        initialize(properties);

        registerUDFs();

        Dataset<FileInputLine> inputLineDataset = fileUtil.getDatasetFromS3(inputFilePath);

        inputLineDataset.show();

        Dataset<FinalResult> finalResultDataset = getFinalResultDataset(inputLineDataset);

        finalResultDataset.show();

        fileUtil.writeDatasetToS3File(finalResultDataset, outputFolderPath);

        javaSparkContext.close();
    }

    private Dataset<FinalResult> getFinalResultDataset(Dataset<FileInputLine> inputLineDataset) {

        return inputLineDataset.withColumn(DOUBLED_COLUMN_NAME,
                callUDF(COLUMN_DOUBLE_UDF_NAME, col(NUMBER_COLUMN_NAME))).as(Encoders.bean(FinalResult.class)
            );
    }

    private void registerUDFs() {
        this.udfUtil.registerColumnDoubleUDF();
    }

    private void initialize(Properties properties) {

        inputFilePath = args[0];
        outputFolderPath = args[1];
        sparkMaster = properties.getProperty("spark.master");

        s3Config = new S3Config();
        s3Config.setProtocol(properties.getProperty("s3.protocol"));
        s3Config.setBucketName(properties.getProperty("s3.bucketName"));
        s3Config.setAccessKey(properties.getProperty("s3.accessKey"));
        s3Config.setAccessSecret(properties.getProperty("s3.accessSecret"));

        javaSparkContext = createJavaSparkContext();
        sqlContext = new SQLContext(javaSparkContext);
        sparkSession = sqlContext.sparkSession();

        udfUtil = new UDFUtil(sqlContext);
        s3Util = new S3Util(s3Config, javaSparkContext);
        fileUtil = new FileUtil(s3Util, sparkSession);
    }

    private void validateArguments() throws ValidationException {

        if (this.args.length < 2) {
            logger.error("Invalid arguments.");
            logger.error("1. Input file path.");
            logger.error("2. Output folder path.");
            logger.error("Example: thetechcheck/input/file.csv thetechcheck/output/");

            throw new ValidationException("Invalid arguments, check help text for instructions.");
        }
    }

    private JavaSparkContext createJavaSparkContext() {

        SparkConf conf = new SparkConf().setAppName("SparkReadAndWriteFromS3POC").setMaster(sparkMaster);

        return new JavaSparkContext(conf);
    }
}
