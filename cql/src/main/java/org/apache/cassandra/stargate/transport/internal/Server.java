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
package org.apache.cassandra.stargate.transport.internal;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.Version;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.stargate.auth.AuthenticationService;
import io.stargate.cql.impl.CqlImpl;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.EventListener;
import io.stargate.db.Persistence;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.messages.EventMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server implements CassandraDaemon.Server {
  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  private static final Logger logger = LoggerFactory.getLogger(Server.class);
  private static final boolean useEpoll = CqlImpl.useEpoll();

  private final ConnectionTracker connectionTracker = new ConnectionTracker();

  private final Connection.Factory connectionFactory =
      new Connection.Factory() {
        public Connection newConnection(
            Channel channel, ProxyInfo proxyInfo, ProtocolVersion version) {
          return new ServerConnection(
              channel, proxyInfo, version, connectionTracker, persistence, authentication);
        }
      };

  public final InetSocketAddress socket;
  public final Persistence persistence;
  public final AuthenticationService authentication;
  public boolean useSSL = false;
  private final AtomicBoolean isRunning = new AtomicBoolean(false);

  private EventLoopGroup workerGroup;

  private Server(Builder builder) {
    this.persistence = builder.persistence;
    this.authentication = builder.authentication;
    this.socket = builder.getSocket();
    this.useSSL = builder.useSSL;
    if (builder.workerGroup != null) {
      workerGroup = builder.workerGroup;
    } else {
      if (useEpoll) workerGroup = new EpollEventLoopGroup();
      else workerGroup = new NioEventLoopGroup();
    }
    this.persistence.registerEventListener(new EventNotifier(this));

    // Please see the comment on setUnsetValue().
    CBUtil.setUnsetValue(persistence.unsetValue());
  }

  public void stop() {
    if (isRunning.compareAndSet(true, false)) close();
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public synchronized void start() {
    if (isRunning()) return;

    // Configure the server.
    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_LINGER, 0)
            .childOption(ChannelOption.SO_KEEPALIVE, TransportDescriptor.getRpcKeepAlive())
            .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
            .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
            .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
    if (workerGroup != null) bootstrap = bootstrap.group(workerGroup);

    if (this.useSSL) {
      final EncryptionOptions clientEnc = TransportDescriptor.getNativeProtocolEncryptionOptions();

      if (clientEnc.optional) {
        logger.info("Enabling optionally encrypted CQL connections between client and server");
        bootstrap.childHandler(new OptionalSecureInitializer(this, clientEnc));
      } else {
        logger.info("Enabling encrypted CQL connections between client and server");
        bootstrap.childHandler(new SecureInitializer(this, clientEnc));
      }
    } else {
      bootstrap.childHandler(new Initializer(this));
    }

    // Bind and start to accept incoming connections.
    logger.info("Using Netty Version: {}", Version.identify().entrySet());
    logger.info(
        "Starting listening for CQL clients on {} ({})...",
        socket,
        this.useSSL ? "encrypted" : "unencrypted");

    ChannelFuture bindFuture = bootstrap.bind(socket);
    if (!bindFuture.awaitUninterruptibly().isSuccess())
      throw new IllegalStateException(
          String.format(
              "Failed to bind port %d on %s.",
              socket.getPort(), socket.getAddress().getHostAddress()),
          bindFuture.cause());

    connectionTracker.allChannels.add(bindFuture.channel());
    isRunning.set(true);
  }

  public int countConnectedClients() {
    return connectionTracker.countConnectedClients();
  }

  public Map<String, Integer> countConnectedClientsByUser() {
    return connectionTracker.countConnectedClientsByUser();
  }

  public List<ConnectedClient> getConnectedClients() {
    List<ConnectedClient> result = new ArrayList<>();
    for (Channel c : connectionTracker.allChannels) {
      Connection conn = c.attr(Connection.attributeKey).get();
      if (conn instanceof ServerConnection)
        result.add(new ConnectedClient((ServerConnection) conn));
    }
    return result;
  }

  public List<ClientStat> recentClientStats() {
    return connectionTracker.protocolVersionTracker.getAll();
  }

  @Override
  public void clearConnectionHistory() {
    connectionTracker.protocolVersionTracker.clear();
  }

  private void close() {
    // Close opened connections
    connectionTracker.closeAll();

    logger.info("Stop listening for CQL clients");
  }

  public static class Builder {
    private final Persistence persistence;
    private final AuthenticationService authentication;
    private EventLoopGroup workerGroup;
    private EventExecutor eventExecutorGroup;
    private boolean useSSL = false;
    private InetAddress hostAddr;
    private int port = -1;
    private InetSocketAddress socket;

    public Builder(Persistence persistence, AuthenticationService authentication) {
      assert persistence != null;
      this.persistence = persistence;
      this.authentication = authentication;
    }

    public Builder withSSL(boolean useSSL) {
      this.useSSL = useSSL;
      return this;
    }

    public Builder withEventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.workerGroup = eventLoopGroup;
      return this;
    }

    public Builder withHost(InetAddress host) {
      this.hostAddr = host;
      this.socket = null;
      return this;
    }

    public Builder withPort(int port) {
      this.port = port;
      this.socket = null;
      return this;
    }

    public Server build() {
      return new Server(this);
    }

    private InetSocketAddress getSocket() {
      if (this.socket != null) return this.socket;
      else {
        if (this.port == -1) throw new IllegalStateException("Missing port number");
        if (this.hostAddr != null) this.socket = new InetSocketAddress(this.hostAddr, this.port);
        else throw new IllegalStateException("Missing host");
        return this.socket;
      }
    }
  }

  public static class ConnectionTracker implements Connection.Tracker {
    // TODO: should we be using the GlobalEventExecutor or defining our own?
    public final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final EnumMap<Event.Type, ChannelGroup> groups = new EnumMap<>(Event.Type.class);
    private final ProtocolVersionTracker protocolVersionTracker = new ProtocolVersionTracker();

    public ConnectionTracker() {
      for (Event.Type type : Event.Type.values())
        groups.put(type, new DefaultChannelGroup(type.toString(), GlobalEventExecutor.INSTANCE));
    }

    public void addConnection(Channel ch, Connection connection) {
      allChannels.add(ch);

      if (ch.remoteAddress() instanceof InetSocketAddress)
        protocolVersionTracker.addConnection(
            ((InetSocketAddress) ch.remoteAddress()).getAddress(), connection.getVersion());
    }

    public void register(Event.Type type, Channel ch) {
      groups.get(type).add(ch);
    }

    public void send(Event event) {
      groups.get(event.type).writeAndFlush(new EventMessage(event));
    }

    void closeAll() {
      allChannels.close().awaitUninterruptibly();
    }

    int countConnectedClients() {
      /*
        - When server is running: allChannels contains all clients' connections (channels)
          plus one additional channel used for the server's own bootstrap.
         - When server is stopped: the size is 0
      */
      return allChannels.size() != 0 ? allChannels.size() - 1 : 0;
    }

    Map<String, Integer> countConnectedClientsByUser() {
      Map<String, Integer> result = new HashMap<>();
      for (Channel c : allChannels) {
        Connection connection = c.attr(Connection.attributeKey).get();
        if (connection instanceof ServerConnection) {
          ServerConnection conn = (ServerConnection) connection;
          Optional<AuthenticatedUser> user = conn.persistenceConnection().loggedUser();
          String name = user.map(AuthenticatedUser::name).orElse(null);
          result.put(name, result.getOrDefault(name, 0) + 1);
        }
      }
      return result;
    }
  }

  // global inflight payload across all channels across all endpoints
  private static final ResourceLimits.Concurrent globalRequestPayloadInFlight =
      new ResourceLimits.Concurrent(
          TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

  public static class EndpointPayloadTracker {
    // inflight payload per endpoint across corresponding channels
    private static final ConcurrentMap<InetAddress, EndpointPayloadTracker>
        requestPayloadInFlightPerEndpoint = new ConcurrentHashMap<>();

    private final AtomicInteger refCount = new AtomicInteger(0);
    private final InetAddress endpoint;

    final ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight =
        new ResourceLimits.EndpointAndGlobal(
            new ResourceLimits.Concurrent(
                TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp()),
            globalRequestPayloadInFlight);

    private EndpointPayloadTracker(InetAddress endpoint) {
      this.endpoint = endpoint;
    }

    public static EndpointPayloadTracker get(InetAddress endpoint) {
      while (true) {
        EndpointPayloadTracker result =
            requestPayloadInFlightPerEndpoint.computeIfAbsent(
                endpoint, EndpointPayloadTracker::new);
        if (result.acquire()) return result;

        requestPayloadInFlightPerEndpoint.remove(endpoint, result);
      }
    }

    public static long getGlobalLimit() {
      return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes();
    }

    public static void setGlobalLimit(long newLimit) {
      TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytes(newLimit);
      long existingLimit =
          globalRequestPayloadInFlight.setLimit(
              TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytes());

      logger.info(
          "Changed native_max_transport_requests_in_bytes from {} to {}", existingLimit, newLimit);
    }

    public static long getEndpointLimit() {
      return TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
    }

    public static void setEndpointLimit(long newLimit) {
      long existingLimit =
          TransportDescriptor.getNativeTransportMaxConcurrentRequestsInBytesPerIp();
      TransportDescriptor.setNativeTransportMaxConcurrentRequestsInBytesPerIp(
          newLimit); // ensure new trackers get the new limit
      for (EndpointPayloadTracker tracker : requestPayloadInFlightPerEndpoint.values())
        existingLimit = tracker.endpointAndGlobalPayloadsInFlight.endpoint().setLimit(newLimit);

      logger.info(
          "Changed native_max_transport_requests_in_bytes_per_ip from {} to {}",
          existingLimit,
          newLimit);
    }

    private boolean acquire() {
      return 0 < refCount.updateAndGet(i -> i < 0 ? i : i + 1);
    }

    public void release() {
      if (-1 == refCount.updateAndGet(i -> i == 1 ? -1 : i - 1))
        requestPayloadInFlightPerEndpoint.remove(endpoint, this);
    }
  }

  private static class Initializer extends ChannelInitializer<Channel> {
    // Stateless handlers
    private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
    private static final Message.ProtocolEncoder messageEncoder = new Message.ProtocolEncoder();
    private static final Frame.InboundBodyTransformer inboundFrameTransformer =
        new Frame.InboundBodyTransformer();
    private static final Frame.OutboundBodyTransformer outboundFrameTransformer =
        new Frame.OutboundBodyTransformer();
    private static final Frame.Encoder frameEncoder = new Frame.Encoder();
    private static final Message.ExceptionHandler exceptionHandler = new Message.ExceptionHandler();
    private static final ConnectionLimitHandler connectionLimitHandler =
        new ConnectionLimitHandler();

    public static final Boolean USE_PROXY_PROTOCOL =
        Boolean.parseBoolean(System.getProperty("stargate.use_proxy_protocol", "false"));

    private final Server server;

    public Initializer(Server server) {
      this.server = server;
    }

    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();

      // Add the ConnectionLimitHandler to the pipeline if configured to do so.
      if (TransportDescriptor.getNativeTransportMaxConcurrentConnections() > 0
          || TransportDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp() > 0) {
        // Add as first to the pipeline so the limit is enforced as first action.
        pipeline.addFirst("connectionLimitHandler", connectionLimitHandler);
      }

      long idleTimeout = TransportDescriptor.nativeTransportIdleTimeout();
      if (idleTimeout > 0) {
        pipeline.addLast(
            "idleStateHandler",
            new IdleStateHandler(false, 0, 0, idleTimeout, TimeUnit.MILLISECONDS) {
              @Override
              protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) {
                logger.info(
                    "Closing client connection {} after timeout of {}ms",
                    channel.remoteAddress(),
                    idleTimeout);
                ctx.close();
              }
            });
      }

      if (USE_PROXY_PROTOCOL) {
        pipeline.addLast("proxyProtocolDecoder", new HAProxyMessageDecoder());
        pipeline.addLast(
            "proxyProtocol",
            new SimpleChannelInboundHandler<HAProxyMessage>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, HAProxyMessage msg)
                  throws Exception {
                Attribute<ProxyInfo> attrProxy = ctx.channel().attr(ProxyInfo.attributeKey);
                attrProxy.set(
                    new ProxyInfo(
                        new InetSocketAddress(msg.destinationAddress(), msg.destinationPort())));
              }
            });
      }

      // pipeline.addLast("debug", new LoggingHandler());

      pipeline.addLast("frameDecoder", new Frame.Decoder(server.connectionFactory));
      pipeline.addLast("frameEncoder", frameEncoder);

      pipeline.addLast("inboundFrameTransformer", inboundFrameTransformer);
      pipeline.addLast("outboundFrameTransformer", outboundFrameTransformer);

      pipeline.addLast("messageDecoder", messageDecoder);
      pipeline.addLast("messageEncoder", messageEncoder);

      pipeline.addLast(
          "executor",
          new Message.Dispatcher(
              TransportDescriptor.useNativeTransportLegacyFlusher(),
              EndpointPayloadTracker.get(
                  ((InetSocketAddress) channel.remoteAddress()).getAddress())));

      // The exceptionHandler will take care of handling exceptionCaught(...) events while still
      // running
      // on the same EventLoop as all previous added handlers in the pipeline. This is important as
      // the used
      // eventExecutorGroup may not enforce strict ordering for channel events.
      // As the exceptionHandler runs in the EventLoop as the previous handlers we are sure all
      // exceptions are
      // correctly handled before the handler itself is removed.
      // See https://issues.apache.org/jira/browse/CASSANDRA-13649
      pipeline.addLast("exceptionHandler", exceptionHandler);
    }
  }

  protected abstract static class AbstractSecureIntializer extends Initializer {
    private final EncryptionOptions encryptionOptions;

    protected AbstractSecureIntializer(Server server, EncryptionOptions encryptionOptions) {
      super(server);
      this.encryptionOptions = encryptionOptions;
    }

    protected final SslHandler createSslHandler(ByteBufAllocator allocator) throws IOException {
      SslContext sslContext =
          SSLFactory.getOrCreateSslContext(
              encryptionOptions,
              encryptionOptions.require_client_auth,
              SSLFactory.SocketType.SERVER);
      return sslContext.newHandler(allocator);
    }
  }

  private static class OptionalSecureInitializer extends AbstractSecureIntializer {
    public OptionalSecureInitializer(Server server, EncryptionOptions encryptionOptions) {
      super(server, encryptionOptions);
    }

    protected void initChannel(final Channel channel) throws Exception {
      super.initChannel(channel);
      channel
          .pipeline()
          .addFirst(
              "sslDetectionHandler",
              new ByteToMessageDecoder() {
                @Override
                protected void decode(
                    ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list)
                    throws Exception {
                  if (byteBuf.readableBytes() < 5) {
                    // To detect if SSL must be used we need to have at least 5 bytes, so return
                    // here and try again
                    // once more bytes a ready.
                    return;
                  }
                  if (SslHandler.isEncrypted(byteBuf)) {
                    // Connection uses SSL/TLS, replace the detection handler with a SslHandler and
                    // so use
                    // encryption.
                    SslHandler sslHandler = createSslHandler(channel.alloc());
                    channelHandlerContext.pipeline().replace(this, "ssl", sslHandler);
                  } else {
                    // Connection use no TLS/SSL encryption, just remove the detection handler and
                    // continue without
                    // SslHandler in the pipeline.
                    channelHandlerContext.pipeline().remove(this);
                  }
                }
              });
    }
  }

  private static class SecureInitializer extends AbstractSecureIntializer {
    public SecureInitializer(Server server, EncryptionOptions encryptionOptions) {
      super(server, encryptionOptions);
    }

    protected void initChannel(Channel channel) throws Exception {
      SslHandler sslHandler = createSslHandler(channel.alloc());
      super.initChannel(channel);
      channel.pipeline().addFirst("ssl", sslHandler);
    }
  }

  private static class LatestEvent {
    public final Event.StatusChange.Status status;
    public final Event.TopologyChange.Change topology;

    private LatestEvent(Event.StatusChange.Status status, Event.TopologyChange.Change topology) {
      this.status = status;
      this.topology = topology;
    }

    @Override
    public String toString() {
      return String.format("Status %s, Topology %s", status, topology);
    }

    public static LatestEvent forStatusChange(Event.StatusChange.Status status, LatestEvent prev) {
      return new LatestEvent(status, prev == null ? null : prev.topology);
    }

    public static LatestEvent forTopologyChange(
        Event.TopologyChange.Change change, LatestEvent prev) {
      return new LatestEvent(prev == null ? null : prev.status, change);
    }
  }

  private static class EventNotifier implements EventListener {
    private final Server server;

    // We keep track of the latest status change events we have sent to avoid sending duplicates
    // since StorageService may send duplicate notifications (CASSANDRA-7816, CASSANDRA-8236,
    // CASSANDRA-9156)
    private final Map<InetAddressAndPort, LatestEvent> latestEvents = new ConcurrentHashMap<>();
    // We also want to delay delivering a NEW_NODE notification until the new node has set its RPC
    // ready
    // state. This tracks the endpoints which have joined, but not yet signalled they're ready for
    // clients
    private final Set<InetAddressAndPort> endpointsPendingJoinedNotification =
        ConcurrentHashMap.newKeySet();

    private EventNotifier(Server server) {
      this.server = server;
    }

    private void send(InetAddressAndPort endpoint, Event.NodeEvent event) {
      if (logger.isTraceEnabled())
        logger.trace(
            "Sending event for endpoint {}, rpc address {}", endpoint, event.nodeAddress());

      // If the endpoint is not the local node, extract the node address
      // and if it is the same as our own RPC broadcast address (which defaults to the rcp address)
      // then don't send the notification. This covers the case of rpc_address set to "localhost",
      // which is not useful to any driver and in fact may cauase serious problems to some drivers,
      // see CASSANDRA-10052
      if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
          && event.nodeAddress().equals(FBUtilities.getJustBroadcastNativeAddress())) return;

      send(event);
    }

    private void send(Event event) {
      server.connectionTracker.send(event);
    }

    // TODO(mpenick): Filter out storage nodes for status/topology events, clients don't need to
    // "see" non-stargate node events

    @Override
    public void onJoinCluster(InetAddressAndPort endpoint) {
      if (server.persistence.isRpcReady(endpoint)) endpointsPendingJoinedNotification.add(endpoint);
      else
        onTopologyChange(
            endpoint, Event.TopologyChange.newNode(server.persistence.getNativeAddress(endpoint)));
    }

    @Override
    public void onLeaveCluster(InetAddressAndPort endpoint) {
      onTopologyChange(
          endpoint,
          Event.TopologyChange.removedNode(server.persistence.getNativeAddress(endpoint)));
    }

    @Override
    public void onMove(InetAddressAndPort endpoint) {
      onTopologyChange(
          endpoint, Event.TopologyChange.movedNode(server.persistence.getNativeAddress(endpoint)));
    }

    @Override
    public void onUp(InetAddressAndPort endpoint) {
      if (endpointsPendingJoinedNotification.remove(endpoint)) onJoinCluster(endpoint);

      onStatusChange(
          endpoint, Event.StatusChange.nodeUp(server.persistence.getNativeAddress(endpoint)));
    }

    public void onDown(InetAddressAndPort endpoint) {
      onStatusChange(
          endpoint, Event.StatusChange.nodeDown(server.persistence.getNativeAddress(endpoint)));
    }

    private void onTopologyChange(InetAddressAndPort endpoint, Event.TopologyChange event) {
      if (logger.isTraceEnabled())
        logger.trace("Topology changed event : {}, {}", endpoint, event.change);

      LatestEvent prev = latestEvents.get(endpoint);
      if (prev == null || prev.topology != event.change) {
        LatestEvent ret =
            latestEvents.put(endpoint, LatestEvent.forTopologyChange(event.change, prev));
        if (ret == prev) send(endpoint, event);
      }
    }

    private void onStatusChange(InetAddressAndPort endpoint, Event.StatusChange event) {
      if (logger.isTraceEnabled())
        logger.trace("Status changed event : {}, {}", endpoint, event.status);

      LatestEvent prev = latestEvents.get(endpoint);
      if (prev == null || prev.status != event.status) {
        LatestEvent ret =
            latestEvents.put(endpoint, LatestEvent.forStatusChange(event.status, null));
        if (ret == prev) send(endpoint, event);
      }
    }

    @Override
    public void onCreateKeyspace(String ksName) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.CREATED, ksName));
    }

    @Override
    public void onCreateTable(String ksName, String cfName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
    }

    @Override
    public void onCreateType(String ksName, String typeName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
    }

    @Override
    public void onCreateFunction(String ksName, String functionName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes));
    }

    @Override
    public void onCreateAggregate(String ksName, String aggregateName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.CREATED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes));
    }

    @Override
    public void onAlterKeyspace(String ksName) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, ksName));
    }

    @Override
    public void onAlterTable(String ksName, String cfName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, ksName, cfName));
    }

    @Override
    public void onAlterType(String ksName, String typeName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, ksName, typeName));
    }

    @Override
    public void onAlterFunction(String ksName, String functionName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes));
    }

    @Override
    public void onAlterAggregate(String ksName, String aggregateName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.UPDATED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes));
    }

    @Override
    public void onDropKeyspace(String ksName) {
      send(new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, ksName));
    }

    @Override
    public void onDropTable(String ksName, String cfName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TABLE, ksName, cfName));
    }

    @Override
    public void onDropType(String ksName, String typeName) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, ksName, typeName));
    }

    @Override
    public void onDropFunction(String ksName, String functionName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.FUNCTION,
              ksName,
              functionName,
              argTypes));
    }

    @Override
    public void onDropAggregate(String ksName, String aggregateName, List<String> argTypes) {
      send(
          new Event.SchemaChange(
              Event.SchemaChange.Change.DROPPED,
              Event.SchemaChange.Target.AGGREGATE,
              ksName,
              aggregateName,
              argTypes));
    }
  }
}
