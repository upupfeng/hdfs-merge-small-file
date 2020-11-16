package com.upupfeng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;

public class Test {

    private static String hdfs = "hdfs://hadoop1:8020/";

    private final static String HEADER_CHAR = "|";
    private final static String SEP = "-";

    private static FileSystem fs;

    static {
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(URI.create(hdfs), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {


//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/mwf"), false);
//        while (listFiles.hasNext()) {
//
//            LocatedFileStatus fileStatus = listFiles.next();
//            String name = fileStatus.getPath().getName();
//            System.out.println(name);
//        }


//        FileStatus fileStatus = fs.getFileStatus(new Path("/"));
//        System.out.println(fileStatus.getPath().getName());

//        FileStatus[] fileStatuses = fs.listStatus(new Path("/mwf"));
//        for (FileStatus status : fileStatuses) {
//            listFiles(status);
//        }
        listFile(new Path("/mwf"));

        System.out.println("ok");


    }

    public static void listFile(Path path) throws IOException {
        listFile(path, 0);
    }

    public static void listFile(Path path, Integer level) throws IOException {
        String lineHeader = getLineHeader(level);

        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                System.out.println(String.format("%sd %s", lineHeader, status.getPath().getName()));
                listFile(status.getPath(), level + 1);
            } else {
                System.out.println(String.format("%s %s", lineHeader, status.getPath().getName()));
            }
        }
    }

    // 从0级目录开始
    public static String getLineHeader(Integer level) {
        StringBuilder sb = new StringBuilder(HEADER_CHAR);
        for (int i = 0; i <= level; i++) {
            sb.append(SEP);
        }
        return sb.toString();
    }

}


