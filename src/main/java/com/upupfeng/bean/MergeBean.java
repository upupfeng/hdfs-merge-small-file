package com.upupfeng.bean;

import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * a bean of merge file
 */
public class MergeBean {

    private Integer mergeId;

    private List<Path> srcPathList;

    private Path dstPath;

    private Long size;

    public MergeBean() {
    }

    public MergeBean(Integer mergeId, List<Path> srcPathList, Path dstPath, Long size) {
        this.mergeId = mergeId;
        this.srcPathList = srcPathList;
        this.size = size;
        this.dstPath = dstPath;
    }

    public Integer getMergeId() {
        return mergeId;
    }

    public void setMergeId(Integer mergeId) {
        this.mergeId = mergeId;
    }

    public List<Path> getSrcPathList() {
        return srcPathList;
    }

    public void setSrcPathList(List<Path> srcPathList) {
        this.srcPathList = srcPathList;
    }

    public Path getDstPath() {
        return dstPath;
    }

    public void setDstPath(Path dstPath) {
        this.dstPath = dstPath;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "MergeBean{" +
                "mergeId=" + mergeId +
                ", srcPathList=" + srcPathList +
                ", dstPath=" + dstPath +
                ", size=" + size +
                '}';
    }
}
