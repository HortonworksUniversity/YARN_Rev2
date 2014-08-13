package com.hortonworks.yarnapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApplicationMaster {
	private static final Logger LOG =
			LoggerFactory.getLogger(ApplicationMaster.class);
	
	
	public static void main(String[] args) {
		LOG.info("Starting ApplicationMaster...");
		
		try {
			Thread.sleep(30000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
