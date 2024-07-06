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
package net.eiroca.sysadm.flume.util.tracker.source;

import java.io.IOException;
import jcifs.CIFSContext;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbRandomAccessFile;
import net.eiroca.ext.library.smb.LibSmb;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

public class TrackedSMB extends TrackedStream {

  private transient SmbRandomAccessFile channel;
  private transient CIFSContext context;
  private transient SmbFile file;

  public TrackedSMB(final SmbFile file, final long commitPos, final WatcherConfig config) {
    this.commitPos = commitPos;
    this.config = config;
    this.file = file;
    context = file.getContext();
    source = file.getURL().toString();
  }

  @Override
  public boolean isChanged() {
    if (id != null) {
      final String curID = LibSmb.getID(file);
      return !id.equals(curID);
    }
    return false;
  }

  @Override
  public boolean isChanged(final String oldID, final long minSize) {
    boolean changed = false;
    String curID = null;
    if (id == null) {
      id = LibSmb.getID(file);
      curID = id;
    }
    if (oldID != null) {
      if (curID == null) {
        curID = LibSmb.getID(file);
      }
      changed = !oldID.equals(curID);
    }
    if ((minSize > 0) && (!changed)) {
      long curSize;
      try {
        curSize = file.length();
        changed = minSize > curSize;
      }
      catch (final SmbException e) {
      }
    }
    return changed;
  }

  @Override
  public void open(final long pos) throws IOException {
    id = LibSmb.getID(file);
    super.open(pos);
    channel = new SmbRandomAccessFile(source, "r", config.shareMode, context);
    commit(pos);
    seek(pos);
  }

  @Override
  public void seek(final long pos) throws IOException {
    bufferPos = TrackedStream.NEED_READING;
    oldBuffer = new byte[0];
    final long size = (pos > 0) ? channelSize(true) : 0;
    if (pos <= size) {
      channel.seek(pos);
      markPos = pos;
    }
    else {
      TrackedSource.logger.info(String.format("Pos: %d  is larger than file size! Restarting from 0, file: %s ID: %s", getCommittedPosition(), getSource(), getID()));
      close();
      open(0);
    }
  }

  @Override
  public void close() {
    TrackedSource.logger.info(String.format("Closing file: %s ID: %s pos: %d", getSource(), getID(), getCommittedPosition()));
    try {
      if (channel != null) {
        channel.close();
        channel = null;
      }
    }
    catch (final IOException e) {
      TrackedSource.logger.error(String.format("Failed closing file: %s ID: %s", source, getID()), e);
    }
    id = null;
  }

  private long channelSize() {
    return channelSize(false);
  }

  private long channelSize(final boolean validate) {
    long size = -1;
    if (channel != null) {
      try {
        size = channel.length();
        if (validate) {
          final long fLen = file.length();
          if (fLen < size) {
            TrackedSource.logger.warn("Unexpected file change. Aborting {}", source);
            size = -1;
          }
        }
      }
      catch (final IOException e) {
        TrackedSource.logger.warn("Unexpected IO error. Aborting {}", source, e);
        size = -1;
      }
    }
    return size;
  }

  private void readBlock() throws IOException {
    byte[] buf;
    final long filePos = channel.getFilePointer();
    final long fileSize = channel.length();
    int blockSize = (int)(fileSize - filePos);
    int blockRead;
    if (blockSize > config.bufferSize) {
      blockSize = config.bufferSize;
    }
    buf = new byte[blockSize];
    blockRead = channel.read(buf);
    if (blockRead == blockSize) {
      buffer = buf;
    }
    else {
      buffer = new byte[blockRead];
      System.arraycopy(buf, 0, buffer, 0, blockRead);
    }
    bufferPos = 0;
    TrackedSource.logger.trace(String.format("Reading %s %d / %d - %d / %d", getID(), blockRead, blockSize, filePos, fileSize));
  }

  @Override
  public boolean readLogicBlock() throws IOException {
    boolean dataRead = false;
    if (channel == null) { return dataRead; }
    if (channel.getFilePointer() < channelSize()) {
      try {
        boolean needRead = true;
        boolean skipped = false;
        int invalidBlocks = 0;
        while (needRead) {
          readBlock();
          bufferPos = checkInvalidBlock(buffer);
          final boolean invalidblock = bufferPos < 0;
          dataRead = !invalidblock;
          if (invalidblock && !skipped) {
            invalidBlocks++;
            setMarkPos(channel.getFilePointer());
            if ((invalidBlocks < config.maxInvalidBlocks)) {
              TrackedSource.logger.trace("{} has nvalid block {}", source, invalidBlocks);
            }
            else {
              final long cLen = channelSize(true);
              long newPos = -1;
              if (cLen > 0) {
                newPos = cLen - (config.keepBlocks * config.bufferSize);
                if (newPos <= channel.getFilePointer()) {
                  newPos = channel.getFilePointer();
                }
                channel.seek(newPos);
                TrackedSource.logger.info("Skipping to {} of {}", newPos, source);
              }
              else {
                TrackedSource.logger.info("Skipping of {}", source);
                close();
                needRead = false;
              }
              setMarkPos(channel.getFilePointer());
              skipped = true;
            }
          }
          else {
            needRead = false;
          }
        }
      }
      catch (final IOException e) {
        TrackedSource.logger.warn("IO error reading file", e);
        close();
      }
    }
    return dataRead;
  }

  @Override
  public boolean isOpen() {
    return channel != null;
  }

}
