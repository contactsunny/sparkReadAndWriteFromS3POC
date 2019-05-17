package com.contactsunny.poc.sparkReadAndWriteFromS3POC.domain;

import org.apache.spark.unsafe.types.UTF8String;

public class FinalResult {

    private String name;
    private int number;
    private int numberDoubled;

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

    public int getNumberDoubled() {
        return numberDoubled;
    }

    public void setNumberDoubled(int numberDoubled) {
        this.numberDoubled = numberDoubled;
    }

    public void setNumberDoubled(UTF8String numberDoubled) {
        this.number = Integer.parseInt(numberDoubled.toString());
    }
}
