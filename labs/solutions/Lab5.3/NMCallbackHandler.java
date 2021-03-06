package com.hortonworks.yarnapp;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NMCallbackHandler implements NMClientAsync.CallbackHandler {

  private static final Logger LOG = LoggerFactory
      .getLogger(NMCallbackHandler.class);

  private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
  private final ApplicationMaster applicationMaster;

  public NMCallbackHandler(ApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
  }

  public void addContainer(ContainerId containerId, Container container) {
    containers.putIfAbsent(containerId, container);
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    LOG.warn("Succeeded to stop Container {}", containerId);
    containers.remove(containerId);
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId,
      ContainerStatus containerStatus) {
    LOG.warn("Container Status: id = {}, status = {}", containerId,
        containerStatus);
  }

  @Override
  public void onContainerStarted(ContainerId containerId,
      Map<String, ByteBuffer> allServiceResponse) {
    LOG.warn("Succeeded to start Container {}", containerId);
    Container container = containers.get(containerId);
    if (container != null) {
      applicationMaster.nmClient.getContainerStatusAsync(containerId,
          container.getNodeId());
    }
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to start Container {}", containerId);
    containers.remove(containerId);
    applicationMaster.numCompletedContainers.incrementAndGet();
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to query the status of Container {}", containerId);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to stop Container {}", containerId);
    containers.remove(containerId);
  }
}