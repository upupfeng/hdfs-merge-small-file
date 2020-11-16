# hdfs-small-file-merge
##背景
使用spark streaming写hdfs，并行度决定了文件的个数，导致会产生许多小文件，需要对小文件进行合并。 \

##说明
使用hadoop api对小文件进行merge \
使用案例：src/main/java/com/upupfeng/demo/MergeFileDemo.java

