/**
 *
 * Copyright (C) 1999-2021 Enrico Croce - AGPL >= 3.0
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
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flume.Context;
import org.apache.flume.Event;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.sysadm.flume.core.util.GenericSink;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.core.util.context.BufferedSinkContext;
import net.eiroca.sysadm.flume.core.util.context.KeyedSinkContext;
import net.eiroca.sysadm.flume.util.ServerConnection;

public class TCPSink extends GenericSink<KeyedSinkContext<SocketAddress>> {

  final StringParameter pServer = new StringParameter(params, "server");
  final IntegerParameter pConnectionRetries = new IntegerParameter(params, "connection-retries", 2);
  final IntegerParameter pConnectionTimeout = new IntegerParameter(params, "connection-timeout", 10);
  final IntegerParameter pConnectionRetryDelay = new IntegerParameter(params, "connection-retry-delay", 10);

  private final Map<SocketAddress, ServerConnection> connections = new HashMap<>();

  private String serverFormat;
  Integer connectionRetries;
  Integer connectionTimeout;
  Integer connectionRetryDelay;

  @Override
  public void configure(final Context context) {
    super.configure(context);
    serverFormat = pServer.get();
    connectionRetries = pConnectionRetries.get();
    connectionTimeout = pConnectionTimeout.get() * 1000;
    connectionRetryDelay = pConnectionRetryDelay.get() * 1000;
  }

  @Override
  public void stop() {
    for (final ServerConnection connection : connections.values()) {
      connection.closeSocket();
    }
    super.stop();
  }

  @Override
  public KeyedSinkContext<SocketAddress> processBegin() throws Exception {
    GenericSink.logger.trace("processBegin()");
    final KeyedSinkContext<SocketAddress> c = new KeyedSinkContext<>(this);
    return c;
  }

  @Override
  protected EventStatus process(final KeyedSinkContext<SocketAddress> context, final Event event, final Map<String, String> headers, final String body) throws Exception {
    GenericSink.logger.trace("processEvent()");
    final String serverName = MacroExpander.expand(serverFormat, headers, body);
    final SocketAddress server = Helper.getServer(serverName);
    context.append(server, event);
    return EventStatus.OK;
  }

  @Override
  public ProcessStatus processEnd(final KeyedSinkContext<SocketAddress> context) throws Exception {
    GenericSink.logger.trace("processEnd()");
    final Map<SocketAddress, BufferedSinkContext> subContexts = context.getSubContexts();
    for (final Entry<SocketAddress, BufferedSinkContext> entry : subContexts.entrySet()) {
      final SocketAddress server = entry.getKey();
      final BufferedSinkContext subContext = entry.getValue();
      final byte[] buffer = subContext.getBuffer();
      if ((buffer != null) && (buffer.length > 0)) {
        final ServerConnection connection = verifyConnection(server);
        try {
          if (connection.isValid()) {
            GenericSink.logger.trace("Sending " + buffer.length + " byte(s) to " + server);
            connection.socket.getOutputStream().write(buffer);
            connection.lastAccess = System.currentTimeMillis();
          }
        }
        catch (final IOException e) {
          GenericSink.logger.error("Socker IO error", e);
          connection.closeSocket();
          throw e;
        }
      }
      subContext.closeSerializer();
    }
    return ProcessStatus.COMMIT;
  }

  private synchronized ServerConnection getConnection(final SocketAddress server) {
    ServerConnection context = connections.get(server);
    if (context == null) {
      context = new ServerConnection(sinkCounter);
      connections.put(server, context);
    }
    return context;
  }

  private ServerConnection verifyConnection(final SocketAddress server) {
    GenericSink.logger.debug("Verifying socket connection " + server);
    final ServerConnection connection = getConnection(server);
    if (!connection.isValid()) {
      connection.closeSocket();
      connection.connectSocket(server, connectionTimeout, connectionRetries, connectionRetryDelay);
    }
    return connection;
  }

}
