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
package net.eiroca.sysadm.flume.util.tracker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flume.Event;
import org.apache.flume.PollableSource.Status;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import net.eiroca.library.core.Helper;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IEventProcessor;
import net.eiroca.sysadm.flume.api.ext.ITrackedSource;
import net.eiroca.sysadm.flume.api.ext.IWatcher;
import net.eiroca.sysadm.flume.api.ext.IWatcherResult;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;
import net.eiroca.sysadm.flume.plugin.TrackerSource;
import net.eiroca.sysadm.flume.util.tracker.watcher.DirectoryWatcher;
import net.eiroca.sysadm.flume.util.tracker.watcher.FileWatcher;
import net.eiroca.sysadm.flume.util.tracker.watcher.SmbDirectoryWatcher;
import net.eiroca.sysadm.flume.util.tracker.watcher.SmbFileWatcher;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

public class TrackerManager extends ConfigurableObject implements LifecycleAware {

  transient private static final Logger logger = Logs.getLogger();

  private static SimpleDateFormat SDF = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS");
  private static final String JSON_ID = "id";
  private static final String JSON_SOURCE = "source";
  private static final String JSON_PREV_CHECK = "prevCheck";
  private static final String JSON_LAST_CHECK = "lastCheck";
  private static final String JSON_ROTATIONS = "rotations";
  private static final String JSON_EVENTS = "events";
  private static final String JSON_EVENTSNEW = "newEvents";
  private static final String JSON_POSITION = "position";
  private static final String JSON_READSIZE = "lastRead";

  private List<IWatcher> watchers = new ArrayList<>();

  private final Map<String, SourceTrack> trackedSources = Maps.newHashMap();

  public TrackerManager() {
    super();
    status = LifecycleState.IDLE;
  }

  //
  private TrackerManagerConfig config;

  @Override
  public boolean isConfigurable() {
    return true;
  }

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    this.config = new TrackerManagerConfig(config, prefix);
    TrackerManager.logger.info("{} configuration: {}", getName(), config);
    try {
      resetFileGroups();
    }
    catch (final IOException e) {
      TrackerManager.logger.error("Unable to configure: {}", e.getMessage(), e);
      status = LifecycleState.ERROR;
    }
  }

  // ------

  private LifecycleState status;

  @Override
  public void start() {
    if (status != LifecycleState.START) {
      status = LifecycleState.START;
    }
  }

  @Override
  public void stop() {
    status = LifecycleState.STOP;
    try {
      writePosition(true);
      for (final SourceTrack source : trackedSources.values()) {
        if (source.tracker != null) {
          source.tracker.close();
          source.tracker = null;
        }
      }
      trackedSources.clear();
    }
    catch (final IOException e) {
      TrackerSource.logger.info("Failed: {}", e.getMessage(), e);
    }
  }

  @Override
  public LifecycleState getLifecycleState() {
    return status;
  }

  // -------

  /**
   * Create a watcher the given directory.
   */
  public void resetFileGroups() throws IOException {
    TrackerManager.logger.info("Initializing fileGroups: {}", getName(), config.watcherConfigs);
    final List<IWatcher> newWatchers = Lists.newArrayList();
    for (final Entry<String, WatcherConfig> e : config.watcherConfigs.entrySet()) {
      IWatcher watcher = null;
      final WatcherConfig conf = e.getValue();
      TrackerManager.logger.trace("building watcher for: {}", conf.name);
      switch (conf.type) {
        case FILE:
          watcher = new FileWatcher(conf);
          break;
        case SMBFILE:
          watcher = new SmbFileWatcher(conf);
          break;
        case SMBREGEX:
          watcher = new SmbDirectoryWatcher(conf);
          break;
        default:
          watcher = new DirectoryWatcher(conf);
          break;
      }
      if (watcher != null) {
        TrackerManager.logger.debug("{} -> {}", watcher.getClass().getSimpleName(), watcher);
        newWatchers.add(watcher);
      }
    }
    watchers = newWatchers;
    TrackerManager.logger.info("Reading status file: {}", config.positionFilePath);
    loadPositionFile(config.positionFilePath);
    updateSources(config.skipToEnd);
  }

  // -------

  public Status flushEvents(final IEventProcessor receiver, final int batchSize) throws IOException {
    TrackerManager.logger.info("Check for new events.");
    boolean hasEvents = false;
    updateSources(false);
    final long now = System.currentTimeMillis();
    for (final SourceTrack ts : trackedSources.values()) {
      writePosition(false);
      if (ts.changed) {
        final boolean flush = (ts.lastCheck > 0) && ((now - ts.lastCheck) > getConfig().inactiveFlush);
        TrackerSource.logger.debug("{}: {} ", flush ? "Flushing" : "Reading", ts.source);
        final ITrackedSource source = ts.tracker;
        if (source != null) {
          try {
            final boolean fileChanged = source.isChanged(ts.id, ts.lastPos);
            if (fileChanged) {
              ts.rotate();
              TrackerManager.logger.info(String.format("File changed: %s [%s]", ts.source, ts.id));
              if (source.isOpen()) {
                source.close();
              }
            }
            if (!source.isOpen()) {
              source.open(ts.lastPos);
              ts.id = source.getID();
            }
            if (processEvents(ts, receiver, batchSize, false, flush)) {
              hasEvents = true;
            }
          }
          catch (final IOException e) {
            TrackerSource.logger.error("Unable to track {}", ts.source, e);
          }
        }
        else {
          TrackerManager.logger.info("Tracker for source {} is missing", ts.source);
        }
      }
    }
    checkInactiveSources(receiver, batchSize);
    Status status;
    if (!hasEvents && config.canBakeOff) {
      status = Status.BACKOFF;
    }
    else {
      status = Status.READY;
    }
    return status;
  }

  private void commit(final SourceTrack ts) throws IOException {
    ts.tracker.commit();
    ts.idle = false;
    ts.checkPoint(System.currentTimeMillis(), ts.tracker.getCommittedPosition());
    writePosition(false);
  }

  private boolean processEvents(final SourceTrack ts, final IEventProcessor receiver, final int batchSize, final boolean backoffWithoutNL, final boolean flush) throws IOException {
    Preconditions.checkNotNull(ts, "TrackedSource must be not NULL");
    Preconditions.checkNotNull(ts.tracker, "Source must be not NULL");
    Preconditions.checkArgument(ts.tracker.isOpen(), "Source must be open");
    boolean hasEvents = false;
    while (true) {
      final List<Event> events = ts.tracker.readEvents(batchSize, backoffWithoutNL, flush);
      if (events.isEmpty()) {
        commit(ts);
        break;
      }
      hasEvents = true;
      ts.addEvents(events.size());
      TrackerSource.logger.info(String.format("Tracking Events: %6d Position: %d source: %s", events.size(), ts.lastPos, ts.me()));
      if (receiver.process(events)) {
        commit(ts);
        if (events.size() < batchSize) {
          break;
        }
      }
      else {
        ts.tracker.rollback();
        break;
      }
    }
    return hasEvents;
  }

  // ----------------------

  public synchronized void checkInactiveSources(final IEventProcessor receiver, final int batchSize) throws IOException {
    final long now = System.currentTimeMillis();
    final ArrayList<String> toDelete = new ArrayList<>();
    for (final SourceTrack ts : trackedSources.values()) {
      if ((!ts.idle) && (getConfig().inactiveClose > 0) && ((now - ts.lastCheck) > getConfig().inactiveClose)) {
        TrackerManager.logger.info("Source idle: {} since: {}", ts.source, (now - ts.lastCheck));
        ts.idle = true;
      }
      if ((!ts.idle) && (ts.tracker != null) && (getConfig().maxOpenTime > 0) && ((now - ts.tracker.getOpeningDate()) > getConfig().maxOpenTime) && ts.tracker.isOpen()) {
        TrackerManager.logger.info("Source maxOpenTime: {} since: {}", ts.source, (now - ts.tracker.getOpeningDate()));
        ts.idle = true;
      }
      if (ts.idle && (ts.tracker != null)) {
        if (ts.tracker.isOpen()) {
          // as file is open, flush last events
          processEvents(ts, receiver, batchSize, false, true);
          ts.tracker.close();
        }
        ts.tracker = null;
      }
      if (ts.tracker == null) {
        if ((now - ts.lastCheck) > getConfig().inactiveDelete) {
          toDelete.add(ts.source);
        }
      }
    }
    for (final String id : toDelete) {
      TrackerManager.logger.info("Removing Source {} ", id);
      trackedSources.remove(id);
    }
  }

  private long lastWrite = 0;

  public synchronized void writePosition(final boolean forceWrite) {
    final long now = System.currentTimeMillis();
    if (forceWrite || ((now - lastWrite) > config.writePosInterval)) {
      TrackerSource.logger.debug("Writing positionFile");
      final File file = new File(getConfig().positionFilePath.toString());
      FileWriter writer = null;
      try {
        writer = new FileWriter(file);
        final String json = toPosInfoJson();
        writer.write(json);
      }
      catch (final Throwable t) {
        TrackerSource.logger.error("Failed writing positionFile", t);
      }
      finally {
        lastWrite = now;
        Helper.close(writer);
      }
    }
  }

  // ----------------------

  private String toPosInfoJson() {
    final List<Map<String, Object>> posInfos = Lists.newArrayList();
    for (final SourceTrack ts : trackedSources.values()) {
      if (ts.tracker != null) {
        ts.lastPos = ts.tracker.getCommittedPosition();
        ts.id = ts.tracker.getID();
      }
      final Map<String, Object> filePos = new HashMap<>();
      filePos.put(TrackerManager.JSON_ID, ts.id);
      filePos.put(TrackerManager.JSON_SOURCE, ts.source);
      filePos.put(TrackerManager.JSON_POSITION, ts.lastPos);
      filePos.put(TrackerManager.JSON_LAST_CHECK, TrackerManager.SDF.format(new Date(ts.lastCheck)));
      if (ts.previousCheck != 0) {
        filePos.put(TrackerManager.JSON_PREV_CHECK, TrackerManager.SDF.format(new Date(ts.previousCheck)));
      }
      if (ts.rotations != 0) {
        filePos.put(TrackerManager.JSON_ROTATIONS, ts.rotations);
      }
      if (ts.lastPos > 0) {
        long lastRead = ts.lastPos - ts.previousPos;
        if (lastRead < 0) {
          lastRead = 0;
        }
        filePos.put(TrackerManager.JSON_READSIZE, lastRead);
      }
      if (ts.events != 0) {
        filePos.put(TrackerManager.JSON_EVENTS, ts.events);
        filePos.put(TrackerManager.JSON_EVENTSNEW, (ts.events - ts.previousEvents));
      }
      posInfos.add(filePos);
    }
    return new Gson().toJson(posInfos);
  }

  /**
   * Load a position file which has the last read position of each file. If the position file
   * exists, update tracked files mapping.
   */
  public void loadPositionFile(final String filePath) {
    String id;
    long lastPos;
    long lastCheck;
    String source;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {
        id = null;
        source = null;
        lastPos = 0;
        lastCheck = 0;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
            case JSON_ID:
              id = jr.nextString();
              break;
            case JSON_SOURCE:
              source = jr.nextString();
              break;
            case JSON_POSITION:
              lastPos = jr.nextLong();
              break;
            case JSON_LAST_CHECK:
              Date lastDate = null;
              try {
                lastDate = TrackerManager.SDF.parse(jr.nextString());
                lastCheck = lastDate.getTime();
              }
              catch (final ParseException e) {
                TrackerManager.logger.info("Invalid date {}, replaced with sysdate ", lastDate);
                lastCheck = System.currentTimeMillis();
              }
              break;
            default: // skip unknown
              jr.nextString();
          }
        }
        jr.endObject();
        if (source != null) {
          SourceTrack ts = trackedSources.get(source);
          if (ts == null) {
            ts = new SourceTrack(id, source);
            ts.checkPoint(lastCheck, lastPos);
            trackedSources.put(ts.source, ts);
          }
        }
      }
      jr.endArray();
    }
    catch (final FileNotFoundException e) {
      TrackerManager.logger.info("File not found: {}, not updating position", filePath);
    }
    catch (final IOException e) {
      TrackerManager.logger.warn("Failed loading positionFile: {} -> {}", filePath, e.getMessage());
      TrackerManager.logger.debug("Failed loading positionFile: {} ", filePath, e);
    }
    finally {
      Helper.close(fr, jr);
    }
  }

  /**
   * Update trackedFiles mapping if a new file is created or appends are detected to the existing
   * file.
   */
  public void updateSources(final boolean skipToEnd) throws IOException {
    TrackerManager.logger.debug("Updating sources");
    final long now = System.currentTimeMillis();
    for (final IWatcher watcher : watchers) {
      final String sourceGroup = watcher.getName();
      TrackerManager.logger.debug("Updating source: {}", sourceGroup);
      for (final IWatcherResult r : watcher.getMatchingFiles()) {
        TrackerManager.logger.debug("Checking file: " + r);
        final String id = r.getID();
        final String path = r.getSource();
        SourceTrack ts = trackedSources.get(path);
        if (ts == null) {
          long startPos;
          long lastChange;
          ts = new SourceTrack(id, path);
          if (skipToEnd) {
            startPos = r.getSize();
            lastChange = r.getUpdateDate();
          }
          else {
            startPos = 0;
            lastChange = 0;
          }
          ts.checkPoint(lastChange, startPos);
          trackedSources.put(ts.source, ts);
          TrackerManager.logger.debug("Adding tracked source: {}", ts);
        }
        boolean changed = false;
        changed = changed || ((config.maxInterval > 0) && ((now - ts.lastCheck) > config.maxInterval));
        changed = changed || ((r.getSize() > 0) && ((ts.lastCheck < r.getUpdateDate()) || (ts.lastPos != r.getSize())));
        if (changed) {
          ts.idle = false;
        }
        else {
          if (ts.lastCheck == 0) {
            ts.checkPoint(now, ts.lastPos);
          }
        }
        ts.changed = changed;
        TrackerManager.logger.debug("Check tracker: {}", ts);
        if ((ts.tracker == null) && (ts.changed)) {
          ts.tracker = trackSource(watcher, sourceGroup, r, id, ts.lastPos);
        }
      }
    }
    TrackerManager.logger.debug("Updating sources completed");
  }

  private ITrackedSource trackSource(final IWatcher watcher, final String fileGroup, final IWatcherResult r, final String fileKey, final long committedPos) {
    final WatcherConfig conf = config.getFileConfig(fileGroup);
    final ITrackedSource tf = r.build(conf, committedPos);
    TrackerManager.logger.debug("New tracker: {}", tf);
    return tf;
  }

  public TrackerManagerConfig getConfig() {
    return config;
  }
}
