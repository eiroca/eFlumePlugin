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

import java.util.List;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.sysadm.flume.api.ext.IEventProcessor;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.util.tracker.TrackerManager;

public class TrackerSource extends AbstractSource implements PollableSource, Configurable, IEventProcessor {

  public static final Logger logger = LoggerFactory.getLogger(TrackerSource.class);

  private SourceCounter sourceCounter;
  private final TrackerManager manager = new TrackerManager();

  final transient private Parameters params = new Parameters();
  final transient private LongParameter pBackoffSleepIncrement = new LongParameter(params, PollableSourceConstants.BACKOFF_SLEEP_INCREMENT, PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
  final transient private LongParameter pMaxBackOffSleepInterval = new LongParameter(params, PollableSourceConstants.MAX_BACKOFF_SLEEP, PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
  final transient private IntegerParameter pBatchSize = new IntegerParameter(params, "batch-size", 100);
  final transient private IntegerParameter pMaxRetries = new IntegerParameter(params, "max-retries", 3);
  final transient private IntegerParameter pMinRetryInterval = new IntegerParameter(params, "min-retry-interval", 5000);
  final transient private IntegerParameter pMaxRetryInterval = new IntegerParameter(params, "max-retry-interval", 30000);
  final transient private BooleanParameter pDiscardOnFail = new BooleanParameter(params, "discard-on-fail", false);

  private long maxBackOffSleepInterval;
  private long backoffSleepIncrement;

  private int batchSize;

  private int maxRetries;
  private int minRetryInterval;
  private int maxRetryInterval;
  private boolean discardOnFail;

  @Override
  public synchronized void configure(final Context context) {
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    FlumeHelper.laodConfig(params, context);
    backoffSleepIncrement = pBackoffSleepIncrement.get();
    maxBackOffSleepInterval = pMaxBackOffSleepInterval.get();
    batchSize = pBatchSize.get();
    maxRetries = pMaxRetries.get();
    minRetryInterval = pMinRetryInterval.get();
    maxRetryInterval = pMaxRetryInterval.get();
    discardOnFail = pDiscardOnFail.get();
    if (manager.isConfigurable()) {
      manager.configure(context.getParameters(), null);
    }
    TrackerSource.logger.debug("{} Manager: {} ", getName(), manager);
  }

  @Override
  public synchronized void start() {
    TrackerSource.logger.info("{} TrackerSource source starting", getName());
    sourceCounter.start();
    manager.start();
    super.start();
    TrackerSource.logger.debug("TrackerSource started");
  }

  @Override
  public synchronized void stop() {
    super.stop();
    manager.stop();
    sourceCounter.stop();
    TrackerSource.logger.info("TrackerSource {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public Status process() {
    Status status;
    try {
      status = manager.flushEvents(this, batchSize);
    }
    catch (final Throwable t) {
      TrackerSource.logger.error("Unable to track sources", t);
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  @Override
  public String toString() {
    return String.format("Tracker %s:  %s", getName(), manager);
  }

  @Override
  public boolean process(final List<Event> events) {
    int retry = 0;
    sourceCounter.addToEventReceivedCount(events.size());
    sourceCounter.incrementAppendBatchReceivedCount();
    boolean success = false;
    int retryInterval = minRetryInterval;
    while (retry < maxRetries) {
      try {
        getChannelProcessor().processEventBatch(events);
        success = true;
        break;
      }
      catch (final ChannelException ex) {
        retry++;
        TrackerSource.logger.warn("The channel is full or unexpected failure. Retry: {}, the source will try again after {} ms", retry, retryInterval);
        Helper.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
    }
    if (success) {
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
    }
    else {
      if (discardOnFail) {
        TrackerSource.logger.info("Discarded {} event(s) due to channel issue", events.size());
        success = true;
      }
      else {
        TrackerSource.logger.warn("Unable to send {} event(s) due to channel issue", events.size());
      }
    }
    return success;
  }

}
