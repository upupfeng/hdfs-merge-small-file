package com.upupfeng.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.upupfeng.bean.MergeBean;

/**
 * A util to merge small file.
 */
public class MergeFileUtil {

    private final static Logger log = Logger.getLogger(MergeFileUtil.class);

    /**
     * merge the file in src into dst
     *
     * @param conf         Hadoop Configuration
     * @param srcPath      the path string of src
     * @param dstPath      the path string of dst
     * @param suffix       the suffix of dst file
     * @param dstFileSize  the byte size of dst file
     * @param concurrent   the concurrent num of merge
     * @param deleteSource whether or delete
     * @throws IOException
     */
    public static void merge(Configuration conf,
                             String srcPath,
                             String dstPath,
                             String suffix,
                             Long dstFileSize,
                             Integer concurrent,
                             Boolean deleteSource) throws IOException {
        log.info("start merge");
        long start = System.currentTimeMillis();

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fileStatuses = getFileStatuses(fs, srcPath);

        ExecutorService threadPool = Executors.newFixedThreadPool(concurrent);

        List<MergeBean> mergeBeanList = getMergeBeanList(fileStatuses, new Path(dstPath), dstFileSize, suffix);

        CountDownLatch countDownLatch = new CountDownLatch(mergeBeanList.size());

        for (MergeBean mb : mergeBeanList) {
            log.info(mb.toString());
            MergeThread thread = new MergeThread(fs, fs, mb, conf, countDownLatch);

            threadPool.submit(thread);

            log.info(String.format("MergeThread %d submitted!", mb.getMergeId()));
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        threadPool.shutdown();

        if (deleteSource) {
            fs.delete(new Path(srcPath), true);
        }

        log.info("--------------------all is ok------------------");

        long end = System.currentTimeMillis();
        log.info(String.format("merge finished in %d s", (start - end) / 1000));
    }

    /**
     * execute merge rely on MergeBean
     *
     * @param srcFs FileSystem of src
     * @param dstFs FileSystem of dst
     * @param mb    MergeBean
     * @param conf  Hadoop Configuration
     * @return true or false
     * @throws IOException
     */
    public static boolean copyMerge(FileSystem srcFs,
                                    FileSystem dstFs,
                                    MergeBean mb,
                                    Configuration conf) throws IOException {
        Path dstPath = mb.getDstPath();
        FSDataOutputStream out = dstFs.create(dstPath);
        List<Path> srcPathList = mb.getSrcPathList();

        try {
            for (Path p : srcPathList) {
                FSDataInputStream in = srcFs.open(p);
                try {
                    IOUtils.copyBytes(in, out, conf, false);
                    log.info(String.format("%s copy completed!", p.getName()));
                } finally {
                    in.close();
                }
            }
        } finally {
            out.close();
        }

        return true;
    }

    /**
     * get the list of MergeBean from FileStatus[]
     *
     * @see MergeFileUtil#getMergeBeanList(FileStatus[], Path, Long, String)
     */
    public static List<MergeBean> getMergeBeanList(FileStatus[] fileStatuses,
                                                   Path dst,
                                                   Long dstFileSize) {
        return getMergeBeanList(fileStatuses, dst, dstFileSize, "");
    }

    /**
     * get the list of MergeBean from FileStatus[]
     *
     * @param fileStatuses FileStatus[]
     * @param dst          the path of dst
     * @param dstFileSize  the size of dst file
     * @param suffix       the suffix of dst file
     * @return List<MergeBean>
     */
    public static List<MergeBean> getMergeBeanList(FileStatus[] fileStatuses,
                                                   Path dst,
                                                   Long dstFileSize,
                                                   String suffix) {
        ArrayList<MergeBean> mergeBeans = new ArrayList<MergeBean>();

        long currentSize = 0;
        int mergeBeanId = 1;
        List<Path> srcPathList = new ArrayList<Path>();
        for (FileStatus status : fileStatuses) {
            if (status.getLen() == 0) {
                continue;
            }
            if ((currentSize = currentSize + status.getLen()) >= dstFileSize) {
                Path dstPath = new Path(dst, String.valueOf(mergeBeanId)).suffix(suffix);
                mergeBeans.add(new MergeBean(mergeBeanId, srcPathList, dstPath, currentSize - status.getLen()));
                mergeBeanId++;
                currentSize = status.getLen();

                srcPathList = new ArrayList<Path>();
            }
            srcPathList.add(status.getPath());
        }

        Path dstPath = new Path(dst, String.valueOf(mergeBeanId)).suffix(suffix);
        mergeBeans.add(new MergeBean(mergeBeanId, srcPathList, dstPath, currentSize));

        return mergeBeans;
    }

    /**
     * get FileStatuses by path
     * eg. getFileStatuses(fs, "/example/20201116")
     *
     * @param fs      FileSystem
     * @param srcPath the path of src
     * @return FileStatus[]
     * @throws IOException
     */
    public static FileStatus[] getFileStatuses(FileSystem fs, String srcPath) throws IOException {
        return fs.listStatus(new Path(srcPath));
    }

    /**
     * merge thread
     */
    public static class MergeThread extends Thread {

        private FileSystem srcFs;
        private FileSystem dstFs;
        private MergeBean mb;
        private Configuration conf;
        private CountDownLatch countDownLatch;

        public MergeThread() {
        }

        public MergeThread(org.apache.hadoop.fs.FileSystem srcFs,
                           org.apache.hadoop.fs.FileSystem dstFs,
                           MergeBean mb,
                           Configuration conf,
                           CountDownLatch countDownLatch) {
            this.srcFs = srcFs;
            this.dstFs = dstFs;
            this.mb = mb;
            this.conf = conf;
            this.countDownLatch = countDownLatch;
        }

        public void run() {
            try {
                log.info(String.format("MergeThread %d is running!", mb.getMergeId()));
                copyMerge(srcFs, dstFs, mb, conf);
                log.info(String.format("MergeThread %d success!", mb.getMergeId()));

                countDownLatch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
