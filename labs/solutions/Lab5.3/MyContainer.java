package com.hortonworks.yarnapp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyContainer {
  private static final Logger LOG = LoggerFactory.getLogger(MyContainer.class);
  private String hostname;
  private YarnConfiguration conf;
  private FileSystem fs;
  private Path inputFile;
  private String searchTerm;
  private Path outputFolder;
  private long start;
  private int length;

  public MyContainer(String[] args) throws IOException {
    hostname = NetUtils.getHostname();
    conf = new YarnConfiguration();
    fs = FileSystem.get(conf);
    inputFile = new Path(args[0]);
    searchTerm = args[1];
    outputFolder = new Path(args[2]);
    start = Long.parseLong(args[3]);
    length = Integer.parseInt(args[4]);
  }

  public static void main(final String[] args) {
    // Execute all operations as the user who submitted the
    // YARN application
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(System
        .getenv("APPUSER"));
    
    ugi.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        LOG.info("Container just started on {}", NetUtils.getHostname());
        try {
          MyContainer container = new MyContainer(args);
          container.run();
        }
        catch (IOException e) {
          e.printStackTrace();
        }
        LOG.info("Container is ending...");
        return null;
      }
    });
  }

  private void run() throws IOException {
    LOG.info("Running Container on {}", this.hostname);
    FSDataInputStream fsdis = fs.open(inputFile);
    fsdis.seek(this.start);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fsdis));
    LOG.info("Reading from {} to {} from {}", start, start + length,
        inputFile.toString());
    String current = "";
    long bytesRead = 0;
    final List<String> results = new ArrayList<>();
    while (bytesRead < this.length && (current = reader.readLine()) != null) {
      bytesRead += current.getBytes().length;
      if (current.contains(searchTerm)) {
        results.add(current);
      }
    }

    Path outputFile = new Path(this.outputFolder + "/result_" + start);
    FSDataOutputStream fsout = fs.create(outputFile, true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fsout));
    for (String match : results) {
      writer.write(match + "\n");
    }
    writer.close();
  }

}
