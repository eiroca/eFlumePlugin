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
package net.eiroca.sysadm.flume.util.tcp;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;

public class MessageParser {

  public interface Callback {

    public void messageComplete(Map<String, Object> metadata, byte[] data) throws Exception;

    public void messageSkipped() throws Exception;
  }

  final Map<String, Object> metadata = new HashMap<>();

  static final int STATE_READ = 2;
  static final int STATE_SKIP = 3;
  static final int STATE_IDLE = 0;
  static final int STATE_HEADER = 1;
  static final int STATE_INVALID = -1;

  Logger logger;

  int state;
  int sizeLength;
  byte[] sizeBuffer;
  byte[] dataBuffer;
  int byteToRead;
  int byteRead;
  int maxBufferLength;
  MessageParser.Callback callback;
  Exception lastException;

  public MessageParser(final Logger logger, final MessageParser.Callback callback, final int sizeLength, final int maxBufferLength) {
    this.logger = logger;
    this.callback = callback;
    this.sizeLength = sizeLength;
    this.maxBufferLength = maxBufferLength;
    init();
  }

  public void init() {
    state = MessageParser.STATE_IDLE;
    sizeBuffer = new byte[sizeLength];
    dataBuffer = null;
    byteToRead = 0;
    byteRead = 0;
    lastException = null;
  }

  private int getMessageSize(final byte[] buffer) {
    int size = 0;
    for (final byte element2 : buffer) {
      final int element = element2 & 0xFF;
      size = (size << 8) + element;
    }
    return size;
  }

  public boolean process(final byte datum) {
    boolean result = true;
    switch (state) {
      case STATE_IDLE: //
        state = MessageParser.STATE_HEADER;
        sizeBuffer[0] = datum;
        byteToRead = sizeLength - 1;
        byteRead = 1;
        logger.trace("Reading Size " + sizeLength);
        break;
      case STATE_HEADER: // Read Message Size
        sizeBuffer[byteRead] = datum;
        byteRead++;
        byteToRead--;
        if (byteToRead == 0) {
          byteToRead = getMessageSize(sizeBuffer);
          if (byteToRead < 1) {
            state = MessageParser.STATE_INVALID;
            final StringBuffer msg = new StringBuffer(1024);
            msg.append("Invalid packet size");
            for (final byte element : sizeBuffer) {
              msg.append(' ').append(element);
            }
            logger.info(msg.toString());
          }
          else {
            logger.trace("Reading Message " + byteToRead);
            byteRead = 0;
            state = byteToRead <= maxBufferLength ? MessageParser.STATE_READ : MessageParser.STATE_SKIP;
            if (state == MessageParser.STATE_READ) {
              dataBuffer = new byte[byteToRead];
            }
          }
        }
        break;
      case STATE_READ: // Read message
        dataBuffer[byteRead] = datum;
        byteToRead--;
        byteRead++;
        if (byteToRead == 0) {
          logger.trace("Process Message " + dataBuffer.length);
          state = MessageParser.STATE_IDLE;
          try {
            callback.messageComplete(metadata, dataBuffer);
          }
          catch (final Exception e) {
            lastException = e;
            result = false;
          }
        }
        break;
      case STATE_SKIP: // SKIP message
        byteToRead--;
        byteRead++;
        if (byteToRead == 0) {
          logger.trace("Skip Message " + byteRead);
          state = MessageParser.STATE_IDLE;
          try {
            callback.messageSkipped();
          }
          catch (final Exception e) {
            lastException = e;
            result = false;
          }
        }
        break;
      default:
        result = false;
    }
    return result;
  }

  public int getState() {
    return state;
  }

  public Exception getLastException() {
    return lastException;
  }

  public void setMetadata(final String key, final Object val) {
    logger.trace("Setting metadata (" + key + "->" + val + ")");
    metadata.put(key, val);
  }

}
