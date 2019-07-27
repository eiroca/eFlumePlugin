/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *******************************************************************************/
package net.eiroca.sysadm.flume.plugin;

import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.api.IEventEncoder;
import net.eiroca.sysadm.flume.api.IEventNotify;
import net.eiroca.sysadm.flume.api.ext.IEventListEncoder;
import net.eiroca.sysadm.flume.core.eventEncoders.EventEncoders;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.util.sqlsource.SqlSourceCounter;
import net.eiroca.sysadm.flume.util.sqlsource.SqlSourceHelper;

/**
 * A Source to read data from a SQL database. This source ask for new data in a table each
 * configured time.
 * <p>
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 */
public class SQLSource extends AbstractSource implements Configurable, PollableSource, IEventNotify {

  private static final Logger logger = LoggerFactory.getLogger(SQLSource.class);

  final Parameters params = new Parameters();
  final transient private LongParameter pBackoffSleepIncrement = new LongParameter(params, PollableSourceConstants.BACKOFF_SLEEP_INCREMENT, PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
  final transient private LongParameter pMaxBackOffSleepInterval = new LongParameter(params, PollableSourceConstants.MAX_BACKOFF_SLEEP, PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
  final transient private StringParameter pEncoder = new StringParameter(params, "encoder", null);
  final transient private IntegerParameter pBatchSize = new IntegerParameter(params, "batch-size", 100);
  final transient private IntegerParameter pQueryDelay = new IntegerParameter(params, "query-delay", 5000); // in
                                                                                                            // ms

  public SqlSourceHelper sqlSourceHelper;
  private SqlSourceCounter sqlSourceCounter;

  private final List<Event> events = new ArrayList<>();

  private long maxBackOffSleepInterval;
  private long backoffSleepIncrement;

  private IEventListEncoder<List<Object>> encoder;
  private int batchSize;
  private int runQueryDelay;

  /**
   * Configure the source, load configuration properties and establish connection with database
   */
  @SuppressWarnings("unchecked")
  @Override
  public void configure(final Context context) {
    SQLSource.logger.info("Reading and processing configuration values for source {}", getName());
    /* Initialize metric counters */
    if (sqlSourceCounter == null) {
      sqlSourceCounter = new SqlSourceCounter("SOURCESQL." + getName());
    }
    /* Initialize configuration parameters */
    sqlSourceHelper = new SqlSourceHelper(context, getName());
    FlumeHelper.laodConfig(params, context);
    final String encoderName = pEncoder.get();
    if (encoderName != null) {
      final IEventEncoder<?> temp = EventEncoders.build(encoderName, context.getParameters(), null, this);
      if (temp instanceof IEventListEncoder<?>) {
        encoder = (IEventListEncoder<List<Object>>)temp;
      }
    }
    if (encoder == null) {
      SQLSource.logger.debug("Encoder '{}' is not valid.", encoderName);
    }
    batchSize = pBatchSize.get();
    runQueryDelay = pQueryDelay.get();
    backoffSleepIncrement = pBackoffSleepIncrement.get();
    maxBackOffSleepInterval = pMaxBackOffSleepInterval.get();
  }

  /**
   * Process a batch of events performing SQL Queries
   */
  @Override
  public Status process() throws EventDeliveryException {
    events.clear();
    sqlSourceCounter.startProcess();
    try {
      final List<List<Object>> result = sqlSourceHelper.executeQuery();
      if (!result.isEmpty()) {
        for (final List<Object> row : result) {
          encoder.encode(row);
        }
        flush();
        sqlSourceCounter.incrementEventCount(result.size());
        sqlSourceHelper.updateStatusFile();
      }
      sqlSourceCounter.endProcess(result.size());
      Thread.sleep(runQueryDelay);
      return Status.READY;
    }
    catch (final InterruptedException e) {
      SQLSource.logger.error("Error procesing row", e);
      return Status.BACKOFF;
    }
  }

  /**
   * Starts the source. Starts the metrics counter.
   */
  @Override
  public void start() {
    SQLSource.logger.info("Starting sql source {} ...", getName());
    sqlSourceCounter.start();
    super.start();
  }

  /**
   * Stop the source. Close database connection and stop metrics counter.
   */
  @Override
  public void stop() {
    SQLSource.logger.info("Stopping sql source {} ...", getName());
    sqlSourceHelper.closeSession();
    sqlSourceCounter.stop();
    super.stop();
  }

  @Override
  public void notifyEvent(final Event e) {
    events.add(e);
    if (events.size() >= batchSize) {
      flush();
    }
  }

  public void flush() {
    if (events.size() > 0) {
      getChannelProcessor().processEventBatch(events);
      events.clear();
    }
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

}
