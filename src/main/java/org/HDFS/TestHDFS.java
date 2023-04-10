package org.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {
    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
        // 第一种方式
//        fs = FileSystem.get(conf);
        // 第二种方式
        fs = FileSystem.get(URI.create("hdfs://localhost:8020"), conf, "zuohang");

    }

    @Test
    public void mkdir() throws Exception {
        Path dir = new Path("/data");
        if (fs.exists(dir)) {
            fs.delete(dir, true);
        }
        boolean mkdirs = fs.mkdirs(dir);
        System.out.println(mkdirs);

    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream input = new BufferedInputStream( new FileInputStream(new File("data/hello.txt")));
        Path outfile = new Path("/data/hello");
        FSDataOutputStream output = fs.create(outfile);
        IOUtils.copyBytes(input, output, conf, true);
    }

    @Test
    public void block() throws Exception {
        Path path = new Path("/data/hello.txt");
        FileStatus fileStatus = fs.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            System.out.println(fileBlockLocation);
            /*
            0,1048576,192.168.0.102
            1048576,140319,192.168.0.102
             */
        }

    }

    @After
    public void close() throws Exception {
        fs.close();
    }
}
