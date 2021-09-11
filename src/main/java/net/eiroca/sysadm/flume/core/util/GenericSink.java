/**
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
package net.eiroca.sysadm.flume.core.util;

import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IEventDecoder;
import net.eiroca.sysadm.flume.api.IEventFilter;
import net.eiroca.sysadm.flume.core.eventDecoders.EventDecoders;
import net.eiroca.sysadm.flume.core.filters.Filters;
import net.eiroca.sysadm.flume.core.util.context.GenericSinkContext;
import net.eiroca.sysadm.flume.plugin.serializer.FormattedSerializer;

abstract public class GenericSink<T extends GenericSinkContext<?>> extends AbstractSink implements Configurable {

  public enum ProcessStatus {
    COMMIT, BAKEOFF, ROLLBACK, FAIL
  }

  public enum EventStatus {
    OK, IGNORED, ERROR, STOP
  }

  protected static final Logger logger = Logs.getLogger();

  final protected Parameters params = new Parameters();
  final protected IntegerParameter pBatchSize = new IntegerParameter(params, "batch-size", 100);
  final protected StringParameter pDecoder = new StringParameter(params, "decoder", EventDecoders.registry.defaultName());
  final protected StringParameter pFilter = new StringParameter(params, "filter", null);
  final protected StringParameter pSerializer = new StringParameter(params, "serializer", FormattedSerializer.Builder.class.getName());

  protected int batchSize;
  protected IEventDecoder<?> decoder;
  protected IEventFilter filter;
  protected String serializerType;
  protected Context serializerContext;

  protected SinkCounter sinkCounter;

  @Override
  public void configure(final Context context) {
    LicenseCheck.runCheck();
    GenericSink.logger.info("Context: {}", context);
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
    FlumeHelper.laodConfig(params, context);
    batchSize = pBatchSize.get();
    final String decoderType = pDecoder.get();
    decoder = EventDecoders.build(decoderType, context.getParameters(), pDecoder.getName() + ".");
    final String filterType = pFilter.get();
    if (filterType != null) {
      filter = Filters.build(filterType, context.getParameters(), pFilter.getName() + ".");
    }
    else {
      filter = null;
    }
    serializerType = pSerializer.get();
    serializerContext = new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
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
    GenericSink.logger.debug("Sink stopped");
    GenericSink.logger.info("Sink metrics: {}", sinkCounter);
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
            case STOP: {
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
      }
      else {
        if ((eventsInBatch >= batchSize)) {
          sinkCounter.incrementBatchCompleteCount();
        }
        else {
          sinkCounter.incrementBatchUnderflowCount();
        }
      }
      if (eventsInBatch == 0) {
        status = Status.BACKOFF;
      }
      else {
        status = Status.READY;
      }
      final ProcessStatus exitStatus = processEnd(context);
      switch (exitStatus) {
        case COMMIT:
          txn.commit();
          sinkCounter.addToEventDrainSuccessCount(eventsInBatch);
          break;
        case BAKEOFF:
          txn.commit();
          sinkCounter.addToEventDrainSuccessCount(eventsInBatch);
          status = Status.BACKOFF;
          break;
        case ROLLBACK:
          txn.rollback();
          break;
        case FAIL:
          txn.rollback();
          status = Status.BACKOFF;
          break;
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

  public T processBegin() throws Exception {
    return null;
  }

  public ProcessStatus processEnd(final T context) throws Exception {
    return ProcessStatus.COMMIT;
  }

  public EventStatus processEvent(final T context, final Event event) throws Exception {
    if (discard(context)) { return EventStatus.IGNORED; }
    final Map<String, String> headers = event.getHeaders();
    if (!accept(context, event, headers)) { return EventStatus.IGNORED; }
    final Object obj = decoder.decode(event);
    final String body = (obj != null) ? String.valueOf(obj) : null;
    GenericSink.logger.trace("Processing {}", event);
    return process(context, event, headers, body);
  }

  protected boolean discard(final T context) {
    return false;
  }

  protected boolean accept(final T context, final Event event, final Map<String, String> headers) {
    return (filter == null) || filter.accept(headers, null);
  }

  protected EventStatus process(final T context, final Event event, final Map<String, String> headers, final String body) throws Exception {
    return EventStatus.IGNORED;
  }

  public String getSerializerType() {
    return serializerType;
  }

  public Context getSerializerContext() {
    return serializerContext;
  }

}
