/**
 *
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package net.eiroca.sysadm.flume.plugin;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.util.tcp.MultiportAcceptor;

/**
 *
 */
public class MultiportTCPSource extends AbstractSource implements EventDrivenSource, Configurable {

  public static final Logger logger = LoggerFactory.getLogger(MultiportTCPSource.class);

  public static final String HEADER_HOSTNAME = "hostname-header";
  public static final String HEADER_DOMAIN = "domain-header";
  public static final String HEADER_SERVER = "server-header";
  public static final String HEADER_HOST = "host-header";
  public static final String HEADER_PORT = "port-header";

  final Parameters params = new Parameters();
  // Hostname to bind to.
  final StringParameter pHost = new StringParameter(params, "bind", null);
  // Port to bind to.
  final ListParameter pPort = new ListParameter(params, "ports");
  final IntegerParameter pMaxBufferLength = new IntegerParameter(params, "max-buffer-length", 4 * 1024 * 1024);
  final StringParameter pPortHeader = new StringParameter(params, MultiportTCPSource.HEADER_PORT, null);
  final StringParameter pHostHeader = new StringParameter(params, MultiportTCPSource.HEADER_HOST, null);
  final StringParameter pServerHeader = new StringParameter(params, MultiportTCPSource.HEADER_SERVER, null);
  final StringParameter pDomainHeader = new StringParameter(params, MultiportTCPSource.HEADER_DOMAIN, null);
  final StringParameter pHostnameHeader = new StringParameter(params, MultiportTCPSource.HEADER_HOSTNAME, null);
  final StringParameter pEncoding = new StringParameter(params, "encoding", "utf-8");

  final IntegerParameter pReadBufferSize = new IntegerParameter(params, "read-buffer-bytes", 4 * 1024);
  final IntegerParameter pNumProcessor = new IntegerParameter(params, "num-processors", 0);
  final IntegerParameter pIdleTime = new IntegerParameter(params, "idle-time", 10);

  private final List<Integer> ports = Lists.newArrayList();
  private String host;
  private NioSocketAcceptor acceptor;
  private int numProcessors;
  private int maxEventSize;
  private int readBufferSize;
  private int idleTime;
  final private HashMap<String, String> headers = new HashMap<>();
  private SourceCounter sourceCounter = null;
  private String encoding;

  public MultiportTCPSource() {
  }

  @Override
  public void configure(final Context context) {
    FlumeHelper.laodConfig(params, context);
    host = pHost.get();
    final String[] portList = pPort.get();
    ports.clear();
    for (final String portStr : portList) {
      final Integer port = Integer.parseInt(portStr);
      ports.add(port);
    }
    encoding = pEncoding.get();
    numProcessors = pNumProcessor.get();
    idleTime = pIdleTime.get();
    maxEventSize = pMaxBufferLength.get();
    headers.put(pHostHeader.getName(), pHostHeader.get());
    headers.put(pHostnameHeader.getName(), pHostnameHeader.get());
    headers.put(pServerHeader.getName(), pServerHeader.get());
    headers.put(pDomainHeader.getName(), pDomainHeader.get());
    headers.put(pPortHeader.getName(), pPortHeader.get());
    readBufferSize = pReadBufferSize.get();
    idleTime = pIdleTime.get();
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    MultiportTCPSource.logger.debug("Config=" + params);
  }

  @Override
  public void start() {
    MultiportTCPSource.logger.info("Starting {}...", this);
    // allow user to specify number of processors to use for thread pool
    if (numProcessors > 0) {
      acceptor = new NioSocketAcceptor(numProcessors);
    }
    else {
      acceptor = new NioSocketAcceptor();
    }
    acceptor.setReuseAddress(true);
    acceptor.getSessionConfig().setReadBufferSize(readBufferSize);
    acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, idleTime);
    final MultiportAcceptor multiportacceptor = new MultiportAcceptor(maxEventSize, getChannelProcessor(), sourceCounter, headers, encoding);
    acceptor.setHandler(multiportacceptor);
    for (final int port : ports) {
      InetSocketAddress addr;
      if (host != null) {
        addr = new InetSocketAddress(host, port);
      }
      else {
        addr = new InetSocketAddress(port);
      }
      try {
        // Not using the one that takes an array because we won't want one bind error affecting the
        // next.
        MultiportTCPSource.logger.info("Binding " + addr);
        acceptor.bind(addr);
      }
      catch (final IOException ex) {
        MultiportTCPSource.logger.error("Could not bind to address: " + String.valueOf(addr), ex);
      }
    }
    sourceCounter.start();
    super.start();
    MultiportTCPSource.logger.info("{} started.", this);
  }

  @Override
  public void stop() {
    MultiportTCPSource.logger.info("Stopping {}...", this);
    acceptor.unbind();
    acceptor.dispose();
    sourceCounter.stop();
    super.stop();
    MultiportTCPSource.logger.info("{} stopped. Metrics: {}", this, sourceCounter);
  }

  @Override
  public String toString() {
    return "Multiport TCP source " + getName();
  }

}
