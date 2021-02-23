package org.apache.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class FIledemo {
    public static void main(String[] args) throws IOException {

        Configuration configured = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(configured);
        fileSystem.mkdirs(new Path("/data/hive/user/test"));

        FSDataOutputStream fos = fileSystem.create(new Path("/data/hive/tmp1.txt"));
        fos.write("abc".getBytes());

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String _path = "hdfs://master:9000/spaceQuota/text.txt";
        FSDataInputStream in = fs.open(new Path(_path));
        IOUtils.copyBytes(in, System.out, 4096, true);
    }
}
