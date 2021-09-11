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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import net.eiroca.library.system.LibFile;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

public class TrackedFile extends TrackedStream {

  private final File file;
  private transient FileChannel channel;

  public TrackedFile(final File file, final long commitPos, final WatcherConfig config) {
    this.commitPos = commitPos;
    this.config = config;
    this.file = file;
    id = null;
    source = file.getAbsolutePath();
  }

  @Override
  public boolean isChanged() {
    if (id != null) {
      final String curID = LibFile.getFileKey(file);
      return !id.equals(curID);
    }
    return false;
  }

  @Override
  public boolean isChanged(final String oldID, final long minSize) {
    boolean changed = false;
    String curID = null;
    if (id == null) {
      id = LibFile.getFileKey(file);
      curID = id;
    }
    if (oldID != null) {
      if (curID == null) {
        curID = LibFile.getFileKey(file);
      }
      changed = !oldID.equals(curID);
    }
    if ((minSize > 0) && (!changed)) {
      final long curSize = file.length();
      changed = minSize > curSize;
    }
    return changed;
  }

  @Override
  public void open(final long pos) throws IOException {
    id = LibFile.getFileKey(file);
    TrackedSource.logger.info(String.format("Opening file: %s ID: %s pos: %d", file, getID(), pos));
    channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
    openDate = System.currentTimeMillis();
    commit(pos);
    seek(pos);
  }

  @Override
  public void seek(final long pos) throws IOException {
    bufferPos = TrackedStream.NEED_READING;
    oldBuffer = new byte[0];
    final long size = (pos > 0) ? channelSize(true) : 0;
    if (pos <= size) {
      channel.position(pos);
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
        size = channel.size();
        if (validate) {
          final long fLen = file.length();
          if (fLen < size) {
            TrackedSource.logger.warn("Unexpected file change. Aborting {}", source);
            size = -1;
          }
        }
      }
      catch (final java.io.IOException e) {
        TrackedSource.logger.warn("Unexpected IO error. Aborting {}", source, e);
        size = -1;
      }
    }
    return size;
  }

  private void readBlock() throws IOException {
    ByteBuffer buf;
    final long filePos = channel.position();
    final long fileSize = channel.size();
    long blockSize = (fileSize - filePos);
    long blockRead;
    if (blockSize > config.bufferSize) {
      blockSize = config.bufferSize;
    }
    buf = ByteBuffer.allocate((int)blockSize);
    blockRead = channel.read(buf);
    buffer = buf.array();
    bufferPos = 0;
    TrackedSource.logger.trace(String.format("Reading %s %d / %d - %d / %d", getID(), blockRead, blockSize, filePos, fileSize));
  }

  @Override
  public boolean readLogicBlock() throws IOException {
    boolean dataRead = false;
    if (channel == null) { return dataRead; }
    if (channel.position() < channelSize()) {
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
            setMarkPos(channel.position());
            if ((invalidBlocks < config.maxInvalidBlocks)) {
              TrackedSource.logger.trace("{} has nvalid block {}", source, invalidBlocks);
            }
            else {
              final long cLen = channelSize(true);
              long newPos = -1;
              if (cLen > 0) {
                newPos = cLen - (config.keepBlocks * config.bufferSize);
                if (newPos <= channel.position()) {
                  newPos = channel.position();
                }
                channel.position(newPos);
                TrackedSource.logger.info("Skipping to {} of {}", newPos, source);
              }
              else {
                TrackedSource.logger.info("Skipping of {}", source);
                close();
                needRead = false;
              }
              setMarkPos(channel.position());
              skipped = true;
            }
          }
          else {
            needRead = false;
          }
        }
      }
      catch (final java.io.IOException e) {
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
