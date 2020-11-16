package com.upupfeng.demo;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

import com.upupfeng.util.MergeFileUtil;

public class MergeFileDemo {

    public static void main(String[] args) {

        String srcPath = "/mwf/data";
        String dstPath = "/mwf/data4";
        Long maxFileSize = 128 * 1024 * 1024L;
        Integer concurrent = 5;
        String suffix = ".csv";
        Configuration conf = new Configuration();

        try {
            MergeFileUtil.merge(conf, srcPath, dstPath, suffix, maxFileSize, concurrent, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
