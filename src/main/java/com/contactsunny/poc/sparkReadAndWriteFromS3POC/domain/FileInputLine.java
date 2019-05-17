package com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain;

import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;

public class FileInputLine implements Serializable {

    private String name;
    private int number;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public void setNumber(UTF8String number) {
        this.number = Integer.parseInt(number.toString());
    }
}
