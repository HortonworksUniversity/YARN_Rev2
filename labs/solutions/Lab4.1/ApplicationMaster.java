package com.hortonworks.yarnapp;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApplicationMaster {
	public class RMCallbackHandler implements CallbackHandler {

		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			LOG.info("Got response from RM for container ask, completed	count = {}", statuses.size());		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			LOG.info("Got response from RM for container ask, allocated	count = {}", containers.size());
		}

		@Override
		public void onShutdownRequest() {
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {}

		@Override
		public float getProgress() {
			float progress = numOfContainers <= 0 ? 0 : (float) numCompletedContainers.get() / numOfContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			amRMClient.stop();
		}

	}

	private static final Logger LOG =
			LoggerFactory.getLogger(ApplicationMaster.class);
	private YarnConfiguration conf;
	private AMRMClientAsync<ContainerRequest> amRMClient;
	private FileSystem fileSystem;
	private int numOfContainers;
	protected AtomicInteger numCompletedContainers =
	new AtomicInteger();
	private volatile boolean done;
	
	public ApplicationMaster(String [] args) throws IOException {
		conf = new YarnConfiguration();
		fileSystem = FileSystem.get(conf);
	}
	
	public void run() throws YarnException, IOException {
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
		amRMClient.init(conf);
		amRMClient.start();
		
		RegisterApplicationMasterResponse response;
		response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
		LOG.info("ApplicationMaster is registered with response: {}", response.toString());
		
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(128);
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);
		for(int i = 1; i <= 5; i++) {
			ContainerRequest ask = new ContainerRequest(capacity,null, null,priority);
			amRMClient.addContainerRequest(ask);
			numOfContainers++;
		}
		
		try {
			Thread.sleep(120000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", null);
		amRMClient.stop();
		
	}
	
	public static void main(String[] args) {
		LOG.info("Starting ApplicationMaster...");
		
		try {
			ApplicationMaster appMaster = new ApplicationMaster(args);
			appMaster.run();
		} catch (IOException | YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
