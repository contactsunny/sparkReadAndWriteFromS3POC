# Apache Spark - Read and Write files from and to AWS S3

This is a simple Java program to illustrate how you can read input datasets from S3 files and write your result datasets to S3 files.

## Inputs

There are two inputs:
1. The input file path, or S3 key.
2. The output S3 folder path.

## Input File

The input file should be a ```.csv``` with the columns ```name``` and ```number```. A sample is included in the root of the project as an example. Make sure you upload this file to an S3 bucket and provide that path as the first input. The file content is as follows:

```csv
name,number
name1,1
name2,2
name3,3
name4,4
```

The first row of the CSV file will be ignored as it is assumed to be the header.

## Running The Project

You need to build the project first before running it. You can build it by running the following command from the root of the project directory:

```shell
mvn clean install
```

After this, run the following command to run the project from the same directory:

```shell
java -jar target/sparkReadAndWriteFromS3POC-1.0-SNAPSHOT.jar thetechecheck/inputFile.csv thetechcheck
```
