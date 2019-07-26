/**
 *
 * Copyright (C) 1999-2019 Enrico Croce - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.util.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.core.util.Flume;
import net.eiroca.sysadm.flume.plugin.MultiportTCPSource;

public class MultiportAcceptor extends IoHandlerAdapter implements MessageParser.Callback {

  private static final Logger logger = LoggerFactory.getLogger(MultiportAcceptor.class);

  private static final String SAVED_PARSER = "savedParser";
  private final ChannelProcessor channelProcessor;
  private final int maxEventSize;
  private final SourceCounter sourceCounter;
  private final HashMap<String, String> headers;
  private final String encoding;

  public MultiportAcceptor(final int maxEventSize, final ChannelProcessor cp, final SourceCounter ctr, final HashMap<String, String> headers, final String encoding) {
    channelProcessor = cp;
    sourceCounter = ctr;
    this.maxEventSize = maxEventSize;
    this.headers = headers;
    this.encoding = encoding;
  }

  @Override
  public void messageSkipped() throws IOException {
    MultiportAcceptor.logger.debug("Event skipped");
  }

  @Override
  public void messageComplete(final Map<String, Object> metadata, final byte[] buffer) throws IOException {
    MultiportAcceptor.logger.debug("Event received");
    sourceCounter.addToEventReceivedCount(1);
    final String message = LibStr.getMessage(buffer, encoding, Flume.BODY_ERROR_MESSAGE);
    final Event event = EventBuilder.withBody(message.getBytes());
    final Map<String, String> headers = event.getHeaders();
    for (final String key : metadata.keySet()) {
      final Object val = metadata.get(key);
      if (val != null) {
        headers.put(key, val.toString());
      }
    }
    // process event
    MultiportAcceptor.logger.trace("Sending event body.length=" + message.length());
    ChannelException ex = null;
    try {
      channelProcessor.processEvent(event);
      sourceCounter.addToEventAcceptedCount(1);
    }
    catch (final ChannelException chEx) {
      ex = chEx;
    }
    if (ex != null) {
      MultiportAcceptor.logger.warn("Error processing event. Exception follows.", ex);
    }
  }

  @Override
  public void exceptionCaught(final IoSession session, final Throwable cause) throws Exception {
    MultiportTCPSource.logger.error("Error in message handler", cause);
    if (cause instanceof Error) {
      Throwables.propagate(cause);
    }
  }

  @Override
  public void sessionCreated(final IoSession session) {
    MultiportTCPSource.logger.debug("Session created: {}", session);
    // Allocate saved buffer when session is created.
    // This allows us to parse an incomplete message and use it on the next request.
    final MessageParser parser = new MessageParser(MultiportAcceptor.logger, this, 4, maxEventSize);
    session.setAttribute(MultiportAcceptor.SAVED_PARSER, parser);
    final String headerP = headers.get(MultiportTCPSource.HEADER_PORT);
    if (headerP != null) {
      parser.setMetadata(headerP, ((InetSocketAddress)session.getLocalAddress()).getPort());
    }
    final String headerH = headers.get(MultiportTCPSource.HEADER_HOST);
    final String headerN = headers.get(MultiportTCPSource.HEADER_HOSTNAME);
    final String headerS = headers.get(MultiportTCPSource.HEADER_SERVER);
    final String headerD = headers.get(MultiportTCPSource.HEADER_DOMAIN);
    if ((headerH != null) || (headerN != null) || (headerS != null) || (headerD != null)) {
      final InetSocketAddress src = (InetSocketAddress)session.getRemoteAddress();
      final String host = src.getHostString();
      final String hostname = ((InetSocketAddress)session.getRemoteAddress()).getHostName();
      final boolean isIP = host.equals(hostname);
      String server;
      String domain;
      if (!isIP) {
        final int pos = hostname.indexOf('.');
        server = (pos > 0) ? hostname.substring(0, pos) : hostname;
        domain = (pos > 0) ? hostname.substring(pos + 1, hostname.length()) : "";
      }
      else {
        server = host;
        domain = "";
      }
      if (headerH != null) {
        parser.setMetadata(headerH, host);
      }
      if (headerN != null) {
        parser.setMetadata(headerN, hostname);
      }
      if (headerS != null) {
        parser.setMetadata(headerS, server.trim().toUpperCase());
      }
      if (headerS != null) {
        parser.setMetadata(headerD, domain.trim().toLowerCase());
      }
    }
  }

  @Override
  public void sessionOpened(final IoSession session) {
    MultiportTCPSource.logger.trace("Session opened: {}", session);
  }

  @Override
  public void sessionClosed(final IoSession session) {
    MultiportTCPSource.logger.debug("Session closed: {}", session);
  }

  @Override
  public void messageReceived(final IoSession session, final Object message) {
    MultiportAcceptor.logger.trace("Message received");
    final IoBuffer buf = (IoBuffer)message;
    final MessageParser parser = (MessageParser)session.getAttribute(MultiportAcceptor.SAVED_PARSER);
    while (buf.hasRemaining()) {
      final byte datum = buf.get();
      parser.process(datum);
    }
  }

}
