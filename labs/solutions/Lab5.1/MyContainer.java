package com.hortonworks.yarnapp;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyContainer {
	private static final Logger LOG = LoggerFactory.getLogger(MyContainer.class);
	private String hostname;
	private YarnConfiguration conf;
	private FileSystem fs;
	
	public MyContainer(String[] args) throws IOException {
		hostname = NetUtils.getHostname();
		conf = new YarnConfiguration();
		fs = FileSystem.get(conf);
	}

	public static void main(String[] args) {
		LOG.info("Container just started on {}", NetUtils.getHostname());
		try {
			MyContainer container = new MyContainer(args);
			container.run();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("Container is ending...");
	}

	private void run() {
		LOG.info("Running Container on {}", this.hostname);
	}

}
