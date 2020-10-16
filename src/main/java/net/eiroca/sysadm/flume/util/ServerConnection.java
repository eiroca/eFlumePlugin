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
package net.eiroca.sysadm.flume.util;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import org.apache.flume.FlumeException;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import net.eiroca.library.core.Helper;
import net.eiroca.library.system.Logs;

public class ServerConnection {

  transient private static final Logger logger = Logs.getLogger();
  /**
   *
   */
  private final SinkCounter counter;

  /**
   * @param tcpSink
   */
  public ServerConnection(final SinkCounter counter) {
    this.counter = counter;
  }

  public long lastAccess;
  public Socket socket;

  public boolean isValid() {
    return (socket != null) && !socket.isClosed() && socket.isConnected();
  }

  public boolean connectSocket(final SocketAddress server, final int connectionTimeout, final int connectionRetries, final int connectionRetryDelayMS) {
    ServerConnection.logger.debug("Connecting to " + server);
    boolean ok = false;
    String error = "Max connection retries performed.";
    for (int retry = 0; (!ok) && (retry < (connectionRetries + 1)); retry++) {
      if (retry > 0) {
        ServerConnection.logger.info("Retrying connection to " + server);
      }
      try {
        socket = new Socket();
        socket.setKeepAlive(true);
        socket.connect(server, connectionTimeout);
        lastAccess = System.currentTimeMillis();
        ok = true;
        error = null;
      }
      catch (final UnknownHostException e) {
        error = "Unable to create TCP connection to " + server + ": " + e.getMessage();
        break;
      }
      catch (final IOException e) {
        ServerConnection.logger.warn("Connection error: ", e);
        Helper.sleep(connectionRetryDelayMS);
      }
    }
    if (ok) {
      counter.incrementConnectionCreatedCount();
      ServerConnection.logger.info("Succesfully Connected to " + server);
    }
    else {
      ServerConnection.logger.error(error);
      counter.incrementConnectionFailedCount();
      closeSocket();
      throw new FlumeException(error);
    }
    return ok;
  }

  public void closeSocket() {
    if (socket != null) {
      ServerConnection.logger.info("Closing socket connection " + socket.getRemoteSocketAddress());
      if (!socket.isClosed()) {
        try {
          socket.close();
          counter.incrementConnectionClosedCount();
        }
        catch (final IOException e) {
          ServerConnection.logger.warn("Unable to close the socket", e);
        }
      }
      socket = null;
    }
  }

}
