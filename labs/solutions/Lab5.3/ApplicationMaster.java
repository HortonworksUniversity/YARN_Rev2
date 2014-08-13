package com.hortonworks.yarnapp;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMaster {

  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationMaster.class);
  private YarnConfiguration conf;
  private AMRMClientAsync<ContainerRequest> amRMClient;
  private FileSystem fileSystem;
  private int numOfContainers;
  protected AtomicInteger numCompletedContainers = new AtomicInteger();
  private volatile boolean done;
  protected NMClientAsync nmClient;
  private NMCallbackHandler containerListener;
  private List<Thread> launchThreads = new ArrayList<>();
  private Path inputFile;
  private String searchTerm;
  private String outputFolder;
  private List<BlockStatus> blockList = new ArrayList<>();

  public ApplicationMaster(String[] args) throws IOException {
    conf = new YarnConfiguration();
    fileSystem = FileSystem.get(conf);
    inputFile = new Path(args[0]);
    searchTerm = args[1];
    outputFolder = args[2];
  }

  public BlockLocation[] getBlockLocations() throws IOException {
    // Read the block information from HDFS
    FileStatus fileStatus = fileSystem.getFileStatus(inputFile);
    LOG.info("File status = {}", fileStatus.toString());
    BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0,
        fileStatus.getLen());
    LOG.info("Number of blocks for {} = {}", inputFile.toString(),
        blocks.length);
    return blocks;
  }

  public void run() throws YarnException, IOException {
    //Remove output folder if it's already there
    LOG.info("Removing outputFolder if present: {}", outputFolder);
    fileSystem.delete(new Path(outputFolder), true);
    
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000,
        new RMCallbackHandler());
    amRMClient.init(conf);
    amRMClient.start();

    RegisterApplicationMasterResponse response;
    response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1,
        "");
    LOG.info("ApplicationMaster is registered with response: {}",
        response.toString());

    containerListener = new NMCallbackHandler(this);
    nmClient = NMClientAsync.createNMClientAsync(containerListener);
    nmClient.init(conf);
    nmClient.start();

    Resource capacity = Records.newRecord(Resource.class);
    capacity.setMemory(128);
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);
    /*
     * for(int i = 1; i <= 5; i++) { ContainerRequest ask = new
     * ContainerRequest(capacity,null, null,priority);
     * amRMClient.addContainerRequest(ask); numOfContainers++; }
     */
    BlockLocation[] blocks = this.getBlockLocations();
    for (BlockLocation block : blocks) {
      ContainerRequest ask = new ContainerRequest(capacity, block.getHosts(),
          null, priority, false);
      numOfContainers++;
      amRMClient.addContainerRequest(ask);
      blockList.add(new BlockStatus(block));
      LOG.info("Asking for Container for block {}", block.toString());
    }

    while (!done && numCompletedContainers.get() < numOfContainers) {
      LOG.info("The number of completed Containers = "
          + this.numCompletedContainers.get());
      try {
        Thread.sleep(2000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // Join all launched threads: needed for when we time
    // out and we need to release containers
    for (Thread launchThread : launchThreads) {
      try {
        launchThread.join(10000);
      }
      catch (InterruptedException e) {
        LOG.info("Exception thrown in thread join: {}", e.getMessage());
        e.printStackTrace();
      }
    }

    LOG.info("Containers have all completed, so shutting down NMClient and AMRMClient...");

    nmClient.stop();
    amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
        "Application complete!", null);
    amRMClient.stop();

  }

  public static void main(final String[] args) throws IOException {
    // Execute all operations as the user who submitted the
    // YARN application. We need to add the YARN security tokens to the new ugi
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(System
        .getenv("APPUSER"));
    Credentials credentials = UserGroupInformation.getCurrentUser()
        .getCredentials();
    ugi.addCredentials(credentials);

    // Now remove the AM->RM token so tasks don't have it
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }

    ugi.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        LOG.info("Starting ApplicationMaster...");
        try {
          ApplicationMaster appMaster = new ApplicationMaster(args);
          appMaster.run();
        }
        catch (IOException | YarnException e) {
          e.printStackTrace();
        }
        LOG.info("ApplicationMaster is ending...");
        return null;
      }
    });
  }

  protected class ContainerLauncher implements Runnable {
    private Container container;

    public ContainerLauncher(Container container) {
      super();
      this.container = container;
    }

    @Override
    public void run() {
      LOG.info("Setting up ContainerLauncher for containerid = {}",
          container.getId());
      Map<String, LocalResource> localResources = new HashMap<>();
      Map<String, String> env = System.getenv();
      LocalResource appJarFile = Records.newRecord(LocalResource.class);
      appJarFile.setType(LocalResourceType.FILE);
      appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
      try {
        appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env
            .get("AMJAR"))));
      }
      catch (URISyntaxException e) {
        e.printStackTrace();
        return;
      }
      appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
      appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
      localResources.put("app.jar", appJarFile);
      LOG.info("Added {} as a local resource to the Container",
          appJarFile.toString());

      ContainerLaunchContext context = Records
          .newRecord(ContainerLaunchContext.class);
      context.setEnvironment(env);
      context.setLocalResources(localResources);
      String command = null;
      try {
        command = this.getLaunchCommand(container);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      List<String> commands = new ArrayList<>();
      commands.add(command);
      context.setCommands(commands);
      LOG.info("Command to execute Container = {}", command);

      nmClient.startContainerAsync(container, context);
      LOG.info("Container {} launched!", container.getId());
    }

    public String getLaunchCommand(Container container) throws IOException {
      String hostname = container.getNodeHttpAddress();
      boolean foundBlock = false;
      BlockStatus blockToProcess = null;
      // Find a BlockStatus that needs to be processed...
      outer: for (BlockStatus current : blockList) {
        synchronized (current) {
          if (!current.isStarted()) {
            for (int i = 0; i < current.getLocation().getHosts().length; i++) {
              String currentHost = current.getLocation().getHosts()[i]
                  + ":8042";
              LOG.info("Comparing {} with container on {}", currentHost,
                  hostname);
              if (currentHost.equals(hostname)) {
                // Assign this BlockStatus to this Container
                blockToProcess = current;
                current.setStarted(true);
                current.setContainer(container);
                foundBlock = true;
                break outer;
              }
            }
          }
        }
      }

      if (foundBlock) {
        LOG.info("Data Locality achieved!!!");
      }
      else {
        LOG.info("Data locality not found - trying another node");
        // Just find any block to process
        for (BlockStatus current : blockList) {
          if (!current.isStarted()) {
            blockToProcess = current;
            current.setStarted(true);
            current.setContainer(container);
            foundBlock = true;
            break;
          }
        }
      }

      Vector<CharSequence> vargs = new Vector<>(30);
      vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
      vargs.add("com.hortonworks.yarnapp.MyContainer ");
      vargs.add(inputFile.toString()); // File to read
      vargs.add(searchTerm); // Search term
      vargs.add(outputFolder); // Output folder
      String offsetString = Long.toString(blockToProcess.getLocation()
          .getOffset());
      String lengthString = Long.toString(blockToProcess.getLocation()
          .getLength());
      LOG.info("Reading block starting at {} and length {}", offsetString,
          lengthString);
      vargs.add(offsetString); // Offset into the file
      vargs.add(lengthString); // Number of bytes to read
      vargs.add("1><LOG_DIR>/MyContainer.stdout");
      vargs.add("2><LOG_DIR>/MyContainer.stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      return command.toString();

    }
  }

  public class RMCallbackHandler implements CallbackHandler {

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      LOG.info("Got response from RM for container ask, completed	count = {}",
          statuses.size());
      for (ContainerStatus status : statuses) {
        numCompletedContainers.incrementAndGet();
        LOG.info("Container completed : {}", status.getContainerId());
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Got response from RM for container ask, allocated	count = {}",
          containers.size());
      for (Container container : containers) {
        LOG.info("Starting Container on {}", container.getNodeHttpAddress());
        ContainerLauncher launcher = new ContainerLauncher(container);
        Thread thread = new Thread(launcher);
        thread.start();
        launchThreads.add(thread);
      }
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
    }

    @Override
    public float getProgress() {
      float progress = numOfContainers <= 0 ? 0
          : (float) numCompletedContainers.get() / numOfContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      done = true;
      amRMClient.stop();
    }

  }

}
