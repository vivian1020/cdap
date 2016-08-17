/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.container;

import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.lang.ClassLoaders;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.internal.Arguments;
import org.apache.twill.internal.BasicTwillContext;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.EnvContainerInfo;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.ServiceMain;
import org.apache.twill.internal.json.ArgumentsCodec;
import org.apache.twill.internal.json.TwillSpecificationAdapter;
import org.apache.twill.internal.logging.Loggings;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;

/**
 * The main class for launching a {@link TwillContainerService}.
 *
 * TODO (TWILL-179) : This class is copied from Apache Twill 0.7.0 release.
 * Need to figure out a better way to control classloader.
 */
public final class TwillContainerMain extends ServiceMain {

  private static final Logger LOG = LoggerFactory.getLogger(TwillContainerMain.class);

  /**
   * Main method for launching a {@link TwillContainerService} which runs
   * a {@link org.apache.twill.api.TwillRunnable}.
   */
  public static void main(String[] args) throws Exception {
    // Begin CDAP modification.
    // Create a MainClassLoader, load this class through that and call doMain.
    ClassLoader classLoader = MainClassLoader.createFromContext();
    if (classLoader == null) {
      LOG.warn("Failed to create CDAP system ClassLoader. Lineage record and Audit Log will not be updated.");
      doMain(args);
      return;
    }

    ClassLoader oldCl = ClassLoaders.setContextClassLoader(classLoader);
    try {
      Method doMain = classLoader.loadClass(TwillContainerMain.class.getName()).getMethod("doMain", String[].class);
      doMain.invoke(null, new Object[] { args });
    } finally {
      ClassLoaders.setContextClassLoader(oldCl);
    }
    // End CDAP modification
  }

  public static void doMain(String[] args) throws Exception {

    // Try to load the secure store from localized file, which AM requested RM to localize it for this container.
    loadSecureStore();

    String zkConnectStr = System.getenv(EnvKeys.TWILL_ZK_CONNECT);
    File twillSpecFile = new File(Constants.Files.TWILL_SPEC);
    RunId appRunId = RunIds.fromString(System.getenv(EnvKeys.TWILL_APP_RUN_ID));
    RunId runId = RunIds.fromString(System.getenv(EnvKeys.TWILL_RUN_ID));
    String runnableName = System.getenv(EnvKeys.TWILL_RUNNABLE_NAME);
    int instanceId = Integer.parseInt(System.getenv(EnvKeys.TWILL_INSTANCE_ID));
    int instanceCount = Integer.parseInt(System.getenv(EnvKeys.TWILL_INSTANCE_COUNT));

    ZKClientService zkClientService = createZKClient(zkConnectStr, System.getenv(EnvKeys.TWILL_APP_NAME));
    ZKDiscoveryService discoveryService = new ZKDiscoveryService(zkClientService);

    ZKClient appRunZkClient = getAppRunZKClient(zkClientService, appRunId);

    TwillSpecification twillSpec = loadTwillSpec(twillSpecFile);

    TwillRunnableSpecification runnableSpec = twillSpec.getRunnables().get(runnableName).getRunnableSpecification();
    ContainerInfo containerInfo = new EnvContainerInfo();
    Arguments arguments = decodeArgs();
    BasicTwillContext context = new BasicTwillContext(
      runId, appRunId, containerInfo.getHost(),
      arguments.getRunnableArguments().get(runnableName).toArray(new String[0]),
      arguments.getArguments().toArray(new String[0]),
      runnableSpec, instanceId, discoveryService, discoveryService, appRunZkClient,
      instanceCount, containerInfo.getMemoryMB(), containerInfo.getVirtualCores()
    );

    ZKClient containerZKClient = getContainerZKClient(zkClientService, appRunId, runnableName);
    Configuration conf = new YarnConfiguration(new HdfsConfiguration(new Configuration()));
    Service service = new TwillContainerService(context, containerInfo, containerZKClient,
                                                runId, runnableSpec, getClassLoader(),
                                                createAppLocation(conf));
    new TwillContainerMain().doMain(
      service,
      new LogFlushService(),
      zkClientService,
      new TwillZKPathService(containerZKClient, runId));
  }

  @Override
  protected String getLoggerLevel(Logger logger) {
    String appLogLevel = System.getenv(EnvKeys.TWILL_APP_LOG_LEVEL);

    return Strings.isNullOrEmpty(appLogLevel) ? super.getLoggerLevel(logger) : appLogLevel;
  }

  private static void loadSecureStore() throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    File file = new File(Constants.Files.CREDENTIALS);
    if (file.exists()) {
      Credentials credentials = new Credentials();
      try (DataInputStream input = new DataInputStream(new FileInputStream(file))) {
        credentials.readTokenStorageStream(input);
      }

      UserGroupInformation.getCurrentUser().addCredentials(credentials);
      LOG.info("Secure store updated from {}", file);
    }
  }

  /**
   * Returns a {@link ZKClient} that namespaced under the given run id.
   */
  private static ZKClient getAppRunZKClient(ZKClient zkClient, RunId appRunId) {
    return ZKClients.namespace(zkClient, String.format("/%s", appRunId));
  }

  private static ZKClient getContainerZKClient(ZKClient zkClient, RunId appRunId, String runnableName) {
    return ZKClients.namespace(zkClient, String.format("/%s/runnables/%s", appRunId, runnableName));
  }

  /**
   * Returns the ClassLoader for the runnable.
   */
  private static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      return ClassLoader.getSystemClassLoader();
    }
    return classLoader;
  }

  private static TwillSpecification loadTwillSpec(File specFile) throws IOException {
    try (Reader reader = Files.newReader(specFile, Charsets.UTF_8)) {
      return TwillSpecificationAdapter.create().fromJson(reader);
    }
  }

  private static Arguments decodeArgs() throws IOException {
    return ArgumentsCodec.decode(Files.newReaderSupplier(new File(Constants.Files.ARGUMENTS), Charsets.UTF_8));
  }

  @Override
  protected String getHostname() {
    return System.getenv(EnvKeys.YARN_CONTAINER_HOST);
  }

  @Override
  protected String getKafkaZKConnect() {
    return System.getenv(EnvKeys.TWILL_LOG_KAFKA_ZK);
  }

  @Override
  protected String getRunnableName() {
    return System.getenv(EnvKeys.TWILL_RUNNABLE_NAME);
  }


  /**
   * Simple service that force flushing logs on stop.
   */
  private static final class LogFlushService extends AbstractService {

    @Override
    protected void doStart() {
      // No-op
      notifyStarted();
    }

    @Override
    protected void doStop() {
      Loggings.forceFlush();
      notifyStopped();
    }
  }
}
