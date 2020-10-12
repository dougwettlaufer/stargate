/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.storage;

import static io.stargate.starter.Starter.STARTED_MESSAGE;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;

import io.stargate.it.storage.StargateParameters.Builder;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit 5 extension for tests that need a Stargate OSGi container running in a separate JVM.
 *
 * <p>Note: this extension requires {@link ExternalStorage} to be activated as well. It is
 * recommended that test classes be annotated with {@link UseStargateContainer} to make sure both
 * extensions are activated in the right order.
 *
 * <p>Note: this extension does not support concurrent test execution.
 *
 * @see StargateSpec
 * @see StargateParameters
 */
public class StargateContainer
    implements BeforeAllCallback,
        AfterAllCallback,
        BeforeEachCallback,
        AfterEachCallback,
        ParameterResolver {
  private static final Logger LOG = LoggerFactory.getLogger(StargateContainer.class);

  private static final File LIB_DIR = new File(System.getProperty("stargate.libdir"));
  private static final int PROCESS_WAIT_MINUTES =
      Integer.getInteger("stargate.test.process.wait.timeout.minutes", 10);

  private static final AtomicBoolean suiteExecuting = new AtomicBoolean();
  private static final AtomicBoolean testExecuting = new AtomicBoolean();

  // the first 10 addresses are reserved for storage nodes
  private static final AtomicInteger stargateAddressStart = new AtomicInteger(11);
  private static final AtomicInteger stargateInstanceSeq = new AtomicInteger();

  private static Container container;

  static {
    Thread containerShutdown =
        new Thread("stargate-container-shutdown") {
          @Override
          public void run() {
            try {
              Container c = container;
              if (c != null) {
                c.stop();
              }
            } catch (Exception e) {
              LOG.warn("Exception during global container shutdown: {}", e.getMessage(), e);
            }
          }
        };

    Runtime.getRuntime().addShutdownHook(containerShutdown);
  }

  private static Container container() {
    if (container == null) {
      throw new IllegalStateException("Stargate container has not been configured");
    }

    return container;
  }

  private static void enter(AtomicBoolean flag) {
    if (!flag.compareAndSet(false, true)) {
      throw new IllegalStateException(
          String.format(
              "Concurrent execution with %s is not supported",
              StargateContainer.class.getSimpleName()));
    }
  }

  private static void leave(AtomicBoolean flag) {
    flag.compareAndSet(true, false);
  }

  private static StargateParameters parameters(StargateSpec spec, ExtensionContext context)
      throws Exception {
    Builder builder = StargateParameters.builder();

    String customizer = spec.parametersCustomizer().trim();
    if (!customizer.isEmpty()) {
      Object testInstance = context.getTestInstance().orElse(null);
      Class<?> testClass = context.getRequiredTestClass();
      Method method = testClass.getMethod(customizer, Builder.class);
      method.invoke(testInstance, builder);
    }

    return builder.build();
  }

  private static synchronized void start(ExtensionContext context) throws Exception {
    ClusterConnectionInfo backend = ExternalStorage.connectionInfo();

    StargateSpec spec =
        context
            .getElement()
            .flatMap(e -> Optional.ofNullable(e.getAnnotation(StargateSpec.class)))
            .orElseGet(
                () ->
                    context
                        .getTestClass()
                        .flatMap(c -> Optional.ofNullable(c.getAnnotation(StargateSpec.class)))
                        .orElse(DefaultSpecHolder.get()));

    StargateParameters params = parameters(spec, context);

    Container c = container;
    if (c != null) {
      if (!c.matches(backend, spec, params)) {
        c.stop();
      } else {
        // reuse container
        LOG.info("Reusing Stargate container for the next test.");
        return;
      }
    }

    c = new Container(backend, spec, params);
    c.start();
    container = c;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    enter(suiteExecuting);
    start(context);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    leave(suiteExecuting);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    enter(testExecuting);
    start(context);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    leave(testExecuting);
  }

  private boolean isStargateConnectionInfo(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType().isAssignableFrom(StargateConnectionInfo.class);
  }

  private boolean isStargateEnvInfo(ParameterContext parameterContext) {
    return parameterContext
        .getParameter()
        .getType()
        .isAssignableFrom(StargateEnvironmentInfo.class);
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return isStargateConnectionInfo(parameterContext) || isStargateEnvInfo(parameterContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    if (isStargateEnvInfo(parameterContext)) {
      return container();
    } else if (isStargateConnectionInfo(parameterContext)) {
      return container().nodes.get(0);
    }

    throw new IllegalStateException("Unknown parameter: " + parameterContext);
  }

  private static boolean isDebug() {
    return getRuntimeMXBean().getInputArguments().toString().contains("-agentlib:jdwp");
  }

  @StargateSpec
  private static final class DefaultSpecHolder {

    private static StargateSpec get() {
      return DefaultSpecHolder.class.getAnnotation(StargateSpec.class);
    }
  }

  private static class Container implements StargateEnvironmentInfo {

    private final UUID id = UUID.randomUUID();
    private final int instanceNum = stargateInstanceSeq.getAndIncrement();
    private final ClusterConnectionInfo backend;
    private final StargateSpec spec;
    private final StargateParameters parameters;
    private final List<Node> nodes = new ArrayList<>();

    private Container(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters)
        throws Exception {
      this.backend = backend;
      this.spec = spec;
      this.parameters = parameters;

      Env env = new Env(spec.nodes());
      for (int i = 0; i < spec.nodes(); i++) {
        nodes.add(new Node(i, instanceNum, backend, env, parameters));
      }
    }

    private void start() {
      for (Node node : nodes) {
        node.start();
      }

      for (Node node : nodes) {
        node.awaitReady();
      }
    }

    private void stop() {
      for (Node node : nodes) {
        node.stopNode();
      }

      for (Node node : nodes) {
        node.awaitExit();
      }
    }

    private boolean matches(
        ClusterConnectionInfo backend, StargateSpec spec, StargateParameters parameters) {
      return this.backend.id().equals(backend.id())
          && this.spec.equals(spec)
          && this.parameters.equals(parameters);
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public List<? extends StargateConnectionInfo> nodes() {
      return nodes;
    }
  }

  private static class Node extends Thread implements StargateConnectionInfo {

    private final UUID id = UUID.randomUUID();
    private final int nodeIndex;
    private final int instanceNum;
    private final String listenAddress;
    private final String clusterName;
    private final CommandLine cmd;
    private final ExecuteWatchdog watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    private final CountDownLatch ready = new CountDownLatch(1);
    private final CountDownLatch exit = new CountDownLatch(1);
    private final int cqlPort;
    private final String datacenter;
    private final String rack;
    private final File cacheDir;

    private Node(
        int nodeIndex,
        int instanceNum,
        ClusterConnectionInfo backend,
        Env env,
        StargateParameters params)
        throws Exception {
      super("stargate-runner-" + nodeIndex);

      this.nodeIndex = nodeIndex;
      this.instanceNum = instanceNum;
      this.listenAddress = env.listenAddress(nodeIndex);
      this.cqlPort = env.cqlPort();
      this.clusterName = backend.clusterName();
      this.datacenter = backend.datacenter();
      this.rack = backend.rack();
      this.cacheDir = env.cacheDir(nodeIndex);

      cmd = new CommandLine("java");
      cmd.addArgument("-Dstargate.auth_api_enable_username_token=true");
      cmd.addArgument("-Dstargate.libdir=" + env.libDir(nodeIndex).getAbsolutePath());
      cmd.addArgument("-Dstargate.bundle.cache.dir=" + cacheDir.getAbsolutePath());

      for (Entry<String, String> e : params.systemProperties().entrySet()) {
        cmd.addArgument("-D" + e.getKey() + "=" + e.getValue());
      }

      if (isDebug()) {
        int debuggerPort = 5100 + nodeIndex;
        cmd.addArgument(
            "-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,"
                + "address=localhost:"
                + debuggerPort);
      }

      cmd.addArgument("-jar");
      cmd.addArgument(env.stargateJar(nodeIndex).getAbsolutePath());
      cmd.addArgument("--cluster-seed");
      cmd.addArgument(backend.seedAddress());
      cmd.addArgument("--seed-port");
      cmd.addArgument(String.valueOf(backend.storagePort()));
      cmd.addArgument("--cluster-name");
      cmd.addArgument(clusterName);
      cmd.addArgument("--cluster-version");
      cmd.addArgument(backend.clusterVersion());
      cmd.addArgument("--dc");
      cmd.addArgument(datacenter);
      cmd.addArgument("--rack");
      cmd.addArgument(rack);

      if (backend.isDse()) {
        cmd.addArgument("--dse");
      }

      if (params.enableAuth()) {
        cmd.addArgument("--enable-auth");
      }

      cmd.addArgument("--listen");
      cmd.addArgument(listenAddress);
      cmd.addArgument("--bind-to-listen-address");
      cmd.addArgument("--cql-port");
      cmd.addArgument(String.valueOf(cqlPort));
      cmd.addArgument("--jmx-port");
      cmd.addArgument(String.valueOf(env.jmxPort(nodeIndex)));
    }

    @Override
    public void run() {
      try {
        LogOutputStream out =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                if (line.contains(STARTED_MESSAGE)) {
                  ready.countDown();
                }

                LOG.info("sg{}-{}> {}", instanceNum, nodeIndex, line);
              }
            };
        LogOutputStream err =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                LOG.error("sg{}-{}> {}", instanceNum, nodeIndex, line);
              }
            };
        Executor executor = new DefaultExecutor();
        executor.setExitValues(new int[] {0, 143}); // normal exit, normal termination by SIGKILL
        executor.setStreamHandler(new PumpStreamHandler(out, err));
        executor.setWatchdog(watchDog);

        try {
          LOG.info("Starting Stargate {}, node {}: {}", instanceNum, nodeIndex, cmd);

          int retValue = executor.execute(cmd);

          LOG.info("Stargate node {} existed with return code {}", nodeIndex, retValue);
        } catch (IOException e) {
          LOG.info("Unable to run Stargate node {}: {}", nodeIndex, e.getMessage(), e);
        }

        try {
          FileUtils.deleteDirectory(cacheDir);
        } catch (IOException e) {
          LOG.info("Unable to delete cache dir for Stargate node {}", nodeIndex, e);
        }
      } finally {
        exit.countDown();
      }
    }

    private void stopNode() {
      LOG.info("Stopping Stargate node {}", nodeIndex);
      watchDog.destroyProcess();
    }

    private void awaitReady() {
      try {
        if (!ready.await(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES)) {
          throw new IllegalStateException("Stargate node not ready: " + nodeIndex);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    private void awaitExit() {
      try {
        if (!exit.await(PROCESS_WAIT_MINUTES, TimeUnit.MINUTES)) {
          throw new IllegalStateException("Stargate node did not exit: " + nodeIndex);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public String id() {
      return id.toString();
    }

    @Override
    public String seedAddress() {
      return listenAddress;
    }

    @Override
    public int cqlPort() {
      return cqlPort;
    }

    @Override
    public String clusterName() {
      return clusterName;
    }

    @Override
    public String datacenter() {
      return datacenter;
    }

    @Override
    public String rack() {
      return rack;
    }
  }

  private static class Env {

    private final List<Integer> ports = new ArrayList<>();
    private final int addressStart;

    private Env(int nodeCount) throws IOException {
      // Note: do not reuse addresses
      addressStart = stargateAddressStart.getAndAdd(nodeCount);

      // Allocate `nodeCount` random ports
      List<ServerSocket> sockets = new ArrayList<>();
      for (int i = 0; i < nodeCount; i++) {
        ServerSocket socket = new ServerSocket(0);
        sockets.add(socket);
        ports.add(socket.getLocalPort());
      }

      for (ServerSocket socket : sockets) {
        socket.close();
      }
    }

    private String listenAddress(int index) {
      return "127.0.0." + (addressStart + index);
    }

    private int jmxPort(int index) {
      return ports.get(index);
    }

    private int cqlPort() {
      return 9043;
    }

    private File libDir(int nodeNumber) throws IOException {
      return LIB_DIR;
    }

    private File stargateJar(int nodeNumber) throws IOException {
      return Arrays.stream(LIB_DIR.listFiles())
          .filter(f -> f.getName().startsWith("stargate-starter"))
          .filter(f -> f.getName().endsWith(".jar"))
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalStateException(
                      "Unable to find Stargate Starter jar in: " + LIB_DIR.getAbsolutePath()));
    }

    public File cacheDir(int nodeIndex) throws IOException {
      return Files.createTempDirectory("stargate-node-" + nodeIndex + "-felix-cache").toFile();
    }
  }
}
