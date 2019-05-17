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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.http.util.ByteArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.regex.LibRegEx;
import net.eiroca.sysadm.flume.api.ext.ITrackedSource;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

abstract public class TrackedSource extends ConfigurableObject implements ITrackedSource {

  protected static final Logger logger = LoggerFactory.getLogger(TrackedSource.class);

  protected WatcherConfig config;
  protected boolean committed = true;

  @Override
  public void commit() throws IOException {
    committed = true;
  }

  @Override
  public Event readEvent() throws IOException {
    final List<Event> events = readEvents(1, false, false);
    if (events.isEmpty()) { return null; }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(final int numEvents) throws IOException {
    return readEvents(numEvents, false, false);
  }

  @Override
  public void rollback() throws IOException {
    committed = true;
  }

  @Override
  public List<Event> readEvents(final int numEvents, final boolean backoffWithoutNL, final boolean flush) throws IOException {
    final List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      final Event event = readSourceEvent(backoffWithoutNL, flush);
      if (event == null) {
        break;
      }
      events.add(event);
    }
    return events;
  }

  private final List<LineResult> lineBuffer = new ArrayList<>();

  private Event readSourceEvent(final boolean backoffWithoutNL, final boolean flush) throws IOException {
    Event event = null;
    final Long offset = getMarkPos();
    while (event == null) {
      final LineResult line = readLine(flush);
      if (line != null) {
        TrackedSource.logger.trace("Line-length: {}", line.line.length);
        if (backoffWithoutNL && !line.lineSepInclude) {
          TrackedSource.logger.debug(String.format("Backing off in file without newline: %s fileKey: %s pos: %d", getSource(), getID(), getMarkPos()));
          seek(offset);
          break;
        }
        else {
          if (checkNewEvent(line)) {
            event = mergeLine(lineBuffer, offset);
            TrackedSource.logger.trace("Merged: {}", event != null ? LibStr.toString(event.getBody()) : "");
            TrackedSource.logger.trace("Adding {}", new String(line.line));
            lineBuffer.add(line);
          }
          else {
            TrackedSource.logger.trace("Adding {}", new String(line.line));
            lineBuffer.add(line);
          }
        }
      }
      if (line == null) {
        TrackedSource.logger.trace("EOF of {}", getSource());
        break;
      }
    }
    final long now = System.currentTimeMillis();
    if ((event == null) && (flush)) {
      event = mergeLine(lineBuffer, offset);
      TrackedSource.logger.trace("Merged exit: {}", event != null ? LibStr.toString(event.getBody()) : "");
    }
    TrackedSource.logger.debug("readEvent @{}: {}", now, event);
    return event;
  }

  private boolean checkNewEvent(final LineResult line) {
    if (getConfig().matcher == null) { return true; }
    final String raw = LibStr.buildString(line.line, getConfig().encoding);
    boolean match = LibRegEx.find(getConfig().matcher, raw, getConfig().sizeLimit);
    if (getConfig().negate) {
      match = !match;
    }
    final boolean makeEvent = getConfig().newEvent;
    final boolean result = match ? makeEvent : !makeEvent;
    TrackedSource.logger.trace("String: {} Negate: {}", raw, getConfig().negate);
    TrackedSource.logger.trace("Matching Result: {} Action: {}", match, result);
    return result;
  }

  private Event mergeLine(final List<LineResult> lines, final long offSet) {
    Event event = null;
    if (lines.size() > 0) {
      final ByteArrayBuffer buf = new ByteArrayBuffer(getConfig().bufferSize);
      final byte[] separator = getConfig().separator;
      for (int i = 0; i < lines.size(); i++) {
        final LineResult line = lines.get(i);
        if ((i > 0) && (separator != null)) {
          buf.append(separator, 0, separator.length);
        }
        buf.append(line.line, 0, line.line.length);
      }
      event = EventBuilder.withBody(buf.toByteArray());
      final Map<String, String> eventHeaders = event.getHeaders();
      if ((config.headers != null) && (!config.headers.isEmpty())) {
        eventHeaders.putAll(config.headers);
      }
      String header;
      header = getConfig().offsetHeaderName;
      if (header != null) {
        eventHeaders.put(header, String.valueOf(offSet));
      }
      header = getConfig().sourceHeaderName;
      if (header != null) {
        eventHeaders.put(header, getSource());
      }
      lines.clear();
    }
    return event;
  }

  @Override
  public WatcherConfig getConfig() {
    return config;
  }

  @Override
  abstract public String getSource();

  abstract public LineResult readLine(boolean flush) throws IOException;

  abstract public long getMarkPos();

  @Override
  abstract public long getOpenDate();
}
