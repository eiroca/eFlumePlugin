/**
 *
 * Copyright (C) 2001-2019 eIrOcA (eNrIcO Croce & sImOnA Burzio) - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.util.tracker.source;

import java.io.IOException;
import net.eiroca.library.core.Helper;

abstract public class TrackedStream extends TrackedSource {

  protected static final int NEED_READING = -1;

  protected String id;
  protected String source;

  protected long openDate;

  protected long markPos;
  protected long commitPos;

  protected byte[] buffer;
  protected byte[] oldBuffer;
  protected int bufferPos;

  protected int checkInvalidBlock(final byte[] buffer) {
    TrackedSource.logger.trace("Checking Invalid block");
    int result = -1;
    for (int i = 0; i < buffer.length; i++) {
      if (buffer[i] != config.invalidChar) {
        result = i;
        break;
      }
    }
    return result;
  }

  abstract public boolean readLogicBlock() throws IOException;

  @Override
  public LineResult readLine(final boolean flush) throws IOException {
    LineResult lineResult = null;
    while (true) {
      if (bufferPos == TrackedStream.NEED_READING) {
        final boolean dataRead = readLogicBlock();
        if (!dataRead) {
          if (flush) {
            final int oldSize = oldBuffer.length;
            TrackedSource.logger.trace("No data from file, {} byte from old buffer", oldSize);
            if (oldSize > 0) {
              lineResult = new LineResult(false, oldBuffer);
              oldBuffer = new byte[0];
              setMarkPos(markPos + lineResult.line.length);
            }
          }
          return lineResult;
        }
      }
      for (int i = bufferPos; i < buffer.length; i++) {
        if (buffer[i] == config.delimiter) {
          int oldLen = oldBuffer.length;
          // Don't copy last byte(NEW_LINE)
          int lineLen = i - bufferPos;
          // For windows, check for CR
          if ((i > 0) && (buffer[i - 1] == config.trimmed)) {
            lineLen -= 1;
          }
          else if ((oldBuffer.length > 0) && (oldBuffer[oldBuffer.length - 1] == config.trimmed)) {
            oldLen -= 1;
          }
          lineResult = new LineResult(true, Helper.concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
          setMarkPos(markPos + (oldBuffer.length + ((i - bufferPos) + 1)));
          oldBuffer = new byte[0];
          if ((i + 1) < buffer.length) {
            bufferPos = i + 1;
          }
          else {
            bufferPos = TrackedStream.NEED_READING;
          }
          break;
        }
      }
      if (lineResult != null) {
        break;
      }
      // NEW_LINE not showed up at the end of the buffer
      oldBuffer = Helper.concatByteArrays(oldBuffer, 0, oldBuffer.length, buffer, bufferPos, buffer.length - bufferPos);
      TrackedSource.logger.trace("Appending data. oldbuffer.length: {}", oldBuffer.length);
      bufferPos = TrackedStream.NEED_READING;
    }
    return lineResult;
  }

  @Override
  public void commit() {
    commit(getMarkPos());
  }

  @Override
  public void commit(final long pos) {
    commitPos = pos;
  }

  @Override
  public void rollback() throws IOException {
    final long lastPos = getCommittedPosition();
    seek(lastPos);
  }

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public String getID() {
    return id;
  }

  @Override
  public long getCommittedPosition() {
    return commitPos;
  }

  @Override
  public long getOpenDate() {
    return openDate;
  }

  @Override
  public long getMarkPos() {
    return markPos;
  }

  public void setMarkPos(final long markPos) {
    this.markPos = markPos;
  }

}
