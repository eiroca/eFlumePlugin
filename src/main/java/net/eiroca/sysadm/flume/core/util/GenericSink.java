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
package net.eiroca.sysadm.flume.core.util;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;

abstract public class GenericSink<T extends GenericSinkContext<?>> extends AbstractSink implements Configurable {

  public enum ProcessStatus {
    COMMIT, BAKEOFF, ROLLBACK, FAIL
  }

  public enum EventStatus {
    OK, IGNORED, ERROR, BAKEOFF
  }

  protected static final Logger logger = Logs.getLogger();

  final protected Parameters params = new Parameters();
  final private StringParameter pEncoding = new StringParameter(params, "encoding", "utf-8");
  final private IntegerParameter pBatchSize = new IntegerParameter(params, "batch-size", 100);

  final private StringParameter pPrioritySource = new StringParameter(params, "priority-source", "%{priority}");
  final private IntegerParameter pPriorityDefault = new IntegerParameter(params, "priority-default", PriorityHelper.DEFAULT_PRIORITY);
  final private StringParameter pPriorityMapping = new StringParameter(params, "priority-mapping", PriorityHelper.DEFAULT_PRIORITY_MAPPING);
  final private IntegerParameter pPriorityMinimum = new IntegerParameter(params, "priority-minimum", 0);
  final private IntegerParameter pPriorityMaximum = new IntegerParameter(params, "priority-maximum", Integer.MAX_VALUE);

  protected SinkCounter sinkCounter;

  protected String encoding;
  protected int batchSize;
  protected PriorityHelper priorityHelper = new PriorityHelper();

  @Override
  public void configure(final Context context) {
    LicenseCheck.runCheck();
    GenericSink.logger.debug("Configure {}...", context);
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
    Flume.laodConfig(params, context);
    encoding = pEncoding.get();
    batchSize = pBatchSize.get();
    priorityHelper.source = pPrioritySource.get();
    priorityHelper.priorityDefault = pPriorityDefault.get();
    priorityHelper.setPriorityMapping(pPriorityMapping.get());
    priorityHelper.priorityMinimum = pPriorityMinimum.get();
    priorityHelper.priorityMaximum = pPriorityMaximum.get();
  }

  @Override
  public void start() {
    super.start();
    GenericSink.logger.debug("Starting {}...", this);
    sinkCounter.start();
  }

  @Override
  public void stop() {
    GenericSink.logger.debug("Stopping {}...", this);
    sinkCounter.stop();
    GenericSink.logger.debug("Sink stopped. Event metrics:{}", sinkCounter);
  }

  public T processBegin() throws Exception {
    return null;
  }

  public EventStatus processEvent(final T context, final Event event) throws Exception {
    return EventStatus.IGNORED;
  }

  public ProcessStatus processEnd(final T context) throws Exception {
    return ProcessStatus.COMMIT;
  }

  @Override
  public Status process() throws EventDeliveryException {
    GenericSink.logger.trace("Processing: " + this);
    long elapsed = System.currentTimeMillis();
    Status status = Status.READY;
    final Channel channel = getChannel();
    final Transaction txn = channel.getTransaction();
    int eventsInBatch = 0;
    T context;
    try {
      context = processBegin();
    }
    catch (final Exception e) {
      GenericSink.logger.error("Sink " + getName() + " fatal error", e);
      return status;
    }
    try {
      txn.begin();
      boolean stop = false;
      for (int i = 0; i < batchSize; i++) {
        final Event event = channel.take();
        if (event != null) {
          sinkCounter.incrementEventDrainAttemptCount();
          switch (processEvent(context, event)) {
            case OK: {
              eventsInBatch++;
              break;
            }
            case IGNORED: {
              break;
            }
            case ERROR: {
              stop = true;
              break;
            }
            case BAKEOFF: {
              eventsInBatch++;
              stop = true;
              break;
            }
          }
          if (stop) {
            break;
          }
        }
        else {
          break;
        }
      }
      if (eventsInBatch == 0) {
        sinkCounter.incrementBatchEmptyCount();
        status = Status.BACKOFF;
      }
      else {
        if ((eventsInBatch >= batchSize)) {
          sinkCounter.incrementBatchCompleteCount();
        }
        else {
          sinkCounter.incrementBatchUnderflowCount();
        }
      }
      final ProcessStatus exitStatus = processEnd(context);
      if ((exitStatus == ProcessStatus.COMMIT) || (exitStatus == ProcessStatus.BAKEOFF)) {
        txn.commit();
        sinkCounter.addToEventDrainSuccessCount(eventsInBatch);
      }
      else {
        txn.rollback();
      }
      if ((exitStatus == ProcessStatus.BAKEOFF) || (exitStatus == ProcessStatus.FAIL)) {
        status = Status.BACKOFF;
      }
      else {
        status = Status.READY;
      }
    }
    catch (final Throwable t) {
      if (txn != null) {
        txn.rollback();
      }
      else if (t instanceof ChannelException) {
        GenericSink.logger.error("Sink " + getName() + ": Unable to get event from channel " + channel.getName() + ". Exception follows.", t);
        status = Status.BACKOFF;
      }
      GenericSink.logger.error("Sink " + getName() + " fatal error", t);
    }
    finally {
      elapsed = System.currentTimeMillis() - elapsed;
      if (eventsInBatch > 0) {
        GenericSink.logger.info(getName() + " processed " + eventsInBatch + " in " + elapsed);
      }
      else {
        GenericSink.logger.debug(getName() + " processed " + eventsInBatch + " in " + elapsed);
      }
      txn.close();
    }
    return status;
  }

}
