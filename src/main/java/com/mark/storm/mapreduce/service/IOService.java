package com.mark.storm.mapreduce.service;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class IOService {
    public static List<String> getLines(String path){
        try {
            return  FileUtils.readLines(new File(path), "utf-8");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
