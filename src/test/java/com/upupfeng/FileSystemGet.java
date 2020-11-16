package com.upupfeng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class FileSystemGet {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS", "hdfs://hadoop1:8020/");
//        fs.defaultFS
//        /** See <a href="{@docRoot}/../core-default.html">core-default.xml</a> */
        FileSystem fs = FileSystem.get(conf);
        System.out.println(fs.getUri().toString());


    }
}
