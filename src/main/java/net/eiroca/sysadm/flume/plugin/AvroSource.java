/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package net.eiroca.sysadm.flume.plugin;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibDecoder;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.ipfilter.IpFilterRule;
import org.jboss.netty.handler.ipfilter.IpFilterRuleHandler;
import org.jboss.netty.handler.ipfilter.PatternRule;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * <p>
 * A {@link Source} implementation that receives Avro events from clients that implement
 * {@link AvroSourceProtocol}.
 * </p>
 * <p>
 * This source forms one half of Flume's tiered collection support. Internally, this source uses
 * Avro's <tt>NettyTransceiver</tt> to listen for, and handle events. It can be paired with the
 * builtin <tt>AvroSink</tt> to create tiered collection topologies. Of course, nothing prevents one
 * from using this source to receive data from other custom built infrastructure that uses the same
 * Avro protocol (specifically {@link AvroSourceProtocol}).
 * </p>
 * <p>
 * Events may be received from the client either singly or in batches.Generally, larger batches are
 * far more efficient, but introduce a slight delay (measured in millis) in delivery. A batch
 * submitted to the configured {@link Channel} atomically (i.e. either all events make it into the
 * channel or none).
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>bind</tt></td>
 * <td>The hostname or IP to which the source will bind.</td>
 * <td>Hostname or IP / String</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which the source will bind and listen for events.</td>
 * <td>TCP port / int</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>threads</tt></td>
 * <td>Max number of threads assigned to thread pool, 0 being unlimited</td>
 * <td>Count / int</td>
 * <td>0(optional)</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class AvroSource extends AbstractSource implements EventDrivenSource, Configurable, AvroSourceProtocol {

  private static final String THREADS = "threads";

  private static final Logger logger = LoggerFactory.getLogger(AvroSource.class);

  private static final String PORT_KEY = "port";
  private static final String BIND_KEY = "bind";
  private static final String COMPRESSION_TYPE = "compression-type";
  private static final String SSL_KEY = "ssl";
  private static final String IP_FILTER_KEY = "ipFilter";
  private static final String IP_FILTER_RULES_KEY = "ipFilterRules";
  private static final String KEYSTORE_KEY = "keystore";
  private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
  private static final String KEYSTORE_TYPE_KEY = "keystore-type";
  private static final String EXCLUDE_PROTOCOLS = "exclude-protocols";
  private int port;
  private String bindAddress;
  private String compressionType;
  private String keystore;
  private String keystorePassword;
  private String keystoreType;
  private final List<String> excludeProtocols = new LinkedList<>();
  private boolean enableSsl = false;
  private boolean enableIpFilter;
  private String patternRuleConfigDefinition;

  private Server server;
  private SourceCounter sourceCounter;

  private int maxThreads;
  private ScheduledExecutorService connectionCountUpdater;

  private List<IpFilterRule> rules;

  @Override
  public void configure(final Context context) {
    Configurables.ensureRequiredNonNull(context, AvroSource.PORT_KEY, AvroSource.BIND_KEY);
    port = context.getInteger(AvroSource.PORT_KEY);
    bindAddress = context.getString(AvroSource.BIND_KEY);
    compressionType = context.getString(AvroSource.COMPRESSION_TYPE, "none");
    try {
      maxThreads = context.getInteger(AvroSource.THREADS, 0);
    }
    catch (final NumberFormatException e) {
      AvroSource.logger.warn("AVRO source\'s \"threads\" property must specify an integer value.", context.getString(AvroSource.THREADS));
    }
    enableSsl = context.getBoolean(AvroSource.SSL_KEY, false);
    keystore = context.getString(AvroSource.KEYSTORE_KEY);
    keystorePassword = context.getString(AvroSource.KEYSTORE_PASSWORD_KEY);
    keystoreType = context.getString(AvroSource.KEYSTORE_TYPE_KEY, "JKS");
    final String excludeProtocolsStr = context.getString(AvroSource.EXCLUDE_PROTOCOLS);
    if (excludeProtocolsStr == null) {
      excludeProtocols.add("SSLv3");
    }
    else {
      excludeProtocols.addAll(Arrays.asList(excludeProtocolsStr.split(" ")));
      if (!excludeProtocols.contains("SSLv3")) {
        excludeProtocols.add("SSLv3");
      }
    }
    if (enableSsl) {
      Preconditions.checkNotNull(keystore, AvroSource.KEYSTORE_KEY + " must be specified when SSL is enabled");
      Preconditions.checkNotNull(keystorePassword, AvroSource.KEYSTORE_PASSWORD_KEY + " must be specified when SSL is enabled");
      try {
        final KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
      }
      catch (final Exception ex) {
        throw new FlumeException("Avro source configured with invalid keystore: " + keystore, ex);
      }
    }
    enableIpFilter = context.getBoolean(AvroSource.IP_FILTER_KEY, false);
    if (enableIpFilter) {
      patternRuleConfigDefinition = context.getString(AvroSource.IP_FILTER_RULES_KEY);
      if ((patternRuleConfigDefinition == null) || patternRuleConfigDefinition.trim().isEmpty()) { throw new FlumeException("ipFilter is configured with true but ipFilterRules is not defined: "); }
      final String[] patternRuleDefinitions = patternRuleConfigDefinition.split(",");
      rules = new ArrayList<>(patternRuleDefinitions.length);
      for (final String patternRuleDefinition : patternRuleDefinitions) {
        rules.add(generateRule(patternRuleDefinition));
      }
    }
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public void start() {
    AvroSource.logger.info("Starting {}...", this);
    final Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);
    final NioServerSocketChannelFactory socketChannelFactory = initSocketChannelFactory();
    final ChannelPipelineFactory pipelineFactory = initChannelPipelineFactory();
    server = new NettyServer(responder, new InetSocketAddress(bindAddress, port), socketChannelFactory, pipelineFactory, null);
    connectionCountUpdater = Executors.newSingleThreadScheduledExecutor();
    server.start();
    sourceCounter.start();
    super.start();
    final NettyServer srv = (NettyServer)server;
    connectionCountUpdater.scheduleWithFixedDelay(() -> sourceCounter.setOpenConnectionCount(Long.valueOf(srv.getNumActiveConnections())), 0, 60, TimeUnit.SECONDS);

    AvroSource.logger.info("Avro source {} started.", getName());
  }

  private NioServerSocketChannelFactory initSocketChannelFactory() {
    NioServerSocketChannelFactory socketChannelFactory;
    if (maxThreads <= 0) {
      socketChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("AvroSource " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
          Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("AvroSource " + NettyTransceiver.class.getSimpleName() + " I/O Worker-%d").build()));
    }
    else {
      socketChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("AvroSource " + NettyTransceiver.class.getSimpleName() + " Boss-%d").build()),
          Executors.newFixedThreadPool(maxThreads, new ThreadFactoryBuilder().setNameFormat("AvroSource " + NettyTransceiver.class.getSimpleName() + "  I/O Worker-%d").build()));
    }
    return socketChannelFactory;
  }

  private ChannelPipelineFactory initChannelPipelineFactory() {
    ChannelPipelineFactory pipelineFactory;
    final boolean enableCompression = compressionType.equalsIgnoreCase("deflate");
    if (enableCompression || enableSsl || enableIpFilter) {
      pipelineFactory = new AdvancedChannelPipelineFactory(enableCompression, enableSsl, keystore, keystorePassword, keystoreType, enableIpFilter, patternRuleConfigDefinition);
    }
    else {
      pipelineFactory = () -> Channels.pipeline();
    }
    return pipelineFactory;
  }

  @Override
  public void stop() {
    AvroSource.logger.info("Avro source {} stopping: {}", getName(), this);
    server.close();
    try {
      server.join();
    }
    catch (final InterruptedException e) {
      AvroSource.logger.info("Avro source " + getName() + ": Interrupted while waiting for Avro server to stop. Exiting. Exception follows.", e);
    }
    sourceCounter.stop();
    connectionCountUpdater.shutdown();
    while (!connectionCountUpdater.isTerminated()) {
      try {
        Thread.sleep(100);
      }
      catch (final InterruptedException ex) {
        AvroSource.logger.error("Interrupted while waiting for connection count executor to terminate", ex);
        Throwables.propagate(ex);
      }
    }
    super.stop();
    AvroSource.logger.info("Avro source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Avro source %s: { bindAddress: %s, port: %d }", getName(), bindAddress, port);
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(final Map<CharSequence, CharSequence> charSeqMap) {
    final Map<String, String> stringMap = new HashMap<>();
    for (final Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public Status append(final AvroFlumeEvent avroEvent) {
    if (AvroSource.logger.isDebugEnabled()) {
      if (LogPrivacyUtil.allowLogRawData()) {
        AvroSource.logger.debug("Avro source {}: Received avro event: {}", getName(), avroEvent);
      }
      else {
        AvroSource.logger.debug("Avro source {}: Received avro event", getName());
      }
    }
    sourceCounter.incrementAppendReceivedCount();
    sourceCounter.incrementEventReceivedCount();
    final Event event = EventBuilder.withBody(avroEvent.getBody().array(), AvroSource.toStringMap(avroEvent.getHeaders()));
    try {
      getChannelProcessor().processEvent(event);
    }
    catch (final ChannelException ex) {
      AvroSource.logger.warn("Avro source {}: Unable to process event. Exception follows.", getName(), ex);
      return Status.FAILED;
    }
    sourceCounter.incrementAppendAcceptedCount();
    sourceCounter.incrementEventAcceptedCount();
    return Status.OK;
  }

  @Override
  public Status appendBatch(final List<AvroFlumeEvent> events) {
    AvroSource.logger.debug("Avro source {}: Received avro event batch of {} events.", getName(), events.size());
    sourceCounter.incrementAppendBatchReceivedCount();
    sourceCounter.addToEventReceivedCount(events.size());
    final List<Event> batch = new ArrayList<>();
    for (final AvroFlumeEvent avroEvent : events) {
      final Event event = EventBuilder.withBody(avroEvent.getBody().array(), AvroSource.toStringMap(avroEvent.getHeaders()));
      batch.add(event);
    }
    AvroSource.logger.debug("Avro source {}: Post processing events.", getName());
    try {
      getChannelProcessor().processEventBatch(batch);
    }
    catch (final Throwable t) {
      AvroSource.logger.error("Avro source {}: Unable to process event batch. Exception follows.", getName(), t);
      if (t instanceof Error) { throw (Error)t; }
      return Status.FAILED;
    }
    sourceCounter.incrementAppendBatchAcceptedCount();
    sourceCounter.addToEventAcceptedCount(events.size());
    return Status.OK;
  }

  private PatternRule generateRule(String patternRuleDefinition) throws FlumeException {
    patternRuleDefinition = patternRuleDefinition.trim();
    // first validate the format
    final int firstColonIndex = patternRuleDefinition.indexOf(":");
    if (firstColonIndex == -1) {
      throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition + "' should look like <'allow'  or 'deny'>:<'ip' or 'name'>:<pattern>");
    }
    else {
      final String ruleAccessFlag = patternRuleDefinition.substring(0, firstColonIndex);
      final int secondColonIndex = patternRuleDefinition.indexOf(":", firstColonIndex + 1);
      if ((!ruleAccessFlag.equals("allow") && !ruleAccessFlag.equals("deny")) || (secondColonIndex == -1)) { throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition + "' should look like <'allow'  or 'deny'>:<'ip' or 'name'>:<pattern>"); }
      final String patternTypeFlag = patternRuleDefinition.substring(firstColonIndex + 1, secondColonIndex);
      if ((!patternTypeFlag.equals("ip") && !patternTypeFlag.equals("name"))) { throw new FlumeException("Invalid ipFilter patternRule '" + patternRuleDefinition + "' should look like <'allow'  or 'deny'>:<'ip' or 'name'>:<pattern>"); }
      final boolean isAllow = ruleAccessFlag.equals("allow");
      final String patternRuleString = (patternTypeFlag.equals("ip") ? "i" : "n") + ":" + patternRuleDefinition.substring(secondColonIndex + 1);
      AvroSource.logger.info("Adding ipFilter PatternRule: " + (isAllow ? "Allow" : "deny") + " " + patternRuleString);
      return new PatternRule(isAllow, patternRuleString);
    }
  }

  /**
   * Factory of SSL-enabled server worker channel pipelines Copied from Avro's
   * org.apache.avro.ipc.TestNettyServerWithSSL test
   */
  private class AdvancedChannelPipelineFactory implements ChannelPipelineFactory {

    private final boolean enableCompression;
    private final boolean enableSsl;
    private final String keystore;
    private final String keystorePassword;
    private final String keystoreType;

    private final boolean enableIpFilter;
    private final String patternRuleConfigDefinition;

    public AdvancedChannelPipelineFactory(final boolean enableCompression, final boolean enableSsl, final String keystore, final String keystorePassword, final String keystoreType, final boolean enableIpFilter, final String patternRuleConfigDefinition) {
      this.enableCompression = enableCompression;
      this.enableSsl = enableSsl;
      this.keystore = keystore;
      this.keystorePassword = keystorePassword;
      this.keystoreType = keystoreType;
      this.enableIpFilter = enableIpFilter;
      this.patternRuleConfigDefinition = patternRuleConfigDefinition;
    }

    private SSLContext createServerSSLContext() {
      try {
        final KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
        // Set up key manager factory to use our key store
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(getAlgorithm());
        kmf.init(ks, keystorePassword.toCharArray());
        final SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);
        return serverContext;
      }
      catch (final Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    }

    private String getAlgorithm() {
      String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
      if (algorithm == null) {
        algorithm = "SunX509";
      }
      return algorithm;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
      final ChannelPipeline pipeline = Channels.pipeline();
      if (enableCompression) {
        final ZlibEncoder encoder = new ZlibEncoder(6);
        pipeline.addFirst("deflater", encoder);
        pipeline.addFirst("inflater", new ZlibDecoder());
      }
      if (enableSsl) {
        final SSLEngine sslEngine = createServerSSLContext().createSSLEngine();
        sslEngine.setUseClientMode(false);
        final List<String> enabledProtocols = new ArrayList<>();
        for (final String protocol : sslEngine.getEnabledProtocols()) {
          if (!excludeProtocols.contains(protocol)) {
            enabledProtocols.add(protocol);
          }
        }
        sslEngine.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
        AvroSource.logger.info("SSLEngine protocols enabled: " + Arrays.asList(sslEngine.getEnabledProtocols()));
        // addFirst() will make SSL handling the first stage of decoding
        // and the last stage of encoding this must be added after
        // adding compression handling above
        pipeline.addFirst("ssl", new SslHandler(sslEngine));
      }
      if (enableIpFilter) {
        AvroSource.logger.info("Setting up ipFilter with the following rule definition: " + patternRuleConfigDefinition);
        final IpFilterRuleHandler ipFilterHandler = new IpFilterRuleHandler();
        ipFilterHandler.addAll(rules);
        AvroSource.logger.info("Adding ipFilter with " + ipFilterHandler.size() + " rules");

        pipeline.addFirst("ipFilter", ipFilterHandler);
      }
      return pipeline;
    }
  }

}
