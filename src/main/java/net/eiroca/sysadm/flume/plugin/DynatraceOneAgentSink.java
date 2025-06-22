/**
 *
 * Copyright (C) 1999-2025 Enrico Croce - AGPL >= 3.0
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

import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import net.eiroca.ext.library.dynatrace.DynatraceBulk;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.GenericSink;
import net.eiroca.sysadm.flume.util.context.DynatraceSinkContext;

public class DynatraceOneAgentSink extends GenericSink<DynatraceSinkContext> {

  private static final String ENCODING = "UTF-8";

  /** Server URL */
  final StringParameter pEndPoint = new StringParameter(params, "server", null, true, false);
  final StringParameter pToken = new StringParameter(params, "token", null);
  final IntegerParameter pVersion = new IntegerParameter(params, "version", 1);
  final IntegerParameter pNumThread = new IntegerParameter(params, "max-threads", 2);
  final IntegerParameter pBulkSize = new IntegerParameter(params, "bulk-size", 16 * 1024);
  final IntegerParameter pQueueLimit = new IntegerParameter(params, "queue-limit", 100);
  final IntegerParameter pBakeOffLimit = new IntegerParameter(params, "bakeoff-limit", -1);

  /** Dynatrace OneAgent Ingest URL to send events to. */
  String endPoint;
  String auth;
  String index;
  String type;
  String id;
  String pipeline;
  int queueLimit;
  int bakeoffLimit;
  boolean useEventTime;

  DynatraceBulk dynatrace;

  @Override
  public void configure(final Context context) {
    super.configure(context);
    endPoint = pEndPoint.get();
    final String token = pToken.get();
    final int bulkSize = pBulkSize.get();
    final int threads = pNumThread.get();
    final int version = pVersion.get();
    dynatrace = new DynatraceBulk(endPoint, version, bulkSize, threads);
    if (token != null) {
      dynatrace.setAuthorization(token);
    }
    dynatrace.setContentType("application/json");
    dynatrace.setAcceptType("application/json");
    try {
      dynatrace.setHeader("[\n".getBytes(ENCODING));
      dynatrace.setFooter("\n]".getBytes(ENCODING));
      dynatrace.setSeparator(",".getBytes(ENCODING));
    }
    catch (UnsupportedEncodingException e) {

    }
    queueLimit = pQueueLimit.get();
    if (queueLimit < 0) {
      queueLimit = 0;
    }
    bakeoffLimit = pBakeOffLimit.get();
    if (bakeoffLimit < 0) {
      bakeoffLimit = Integer.MAX_VALUE;
    }
  }

  @Override
  public DynatraceSinkContext processBegin() throws Exception {
    final DynatraceSinkContext context = new DynatraceSinkContext(this);
    dynatrace.open();
    final int queueSize = (queueLimit > 0) ? dynatrace.getQueueSize() : 0;
    context.discard = false;
    if (queueSize > queueLimit) {
      GenericSink.logger.error("Indexer overload {} - discarding events", queueSize);
      context.discard = true;
    }
    else if (queueSize > 0) {
      GenericSink.logger.warn("Indexer overload {}", queueSize);
    }
    return context;
  }

  @Override
  protected boolean discard(final DynatraceSinkContext context) {
    return context.discard;
  }

  @Override
  protected EventStatus process(final DynatraceSinkContext context, final Event event, final Map<String, String> headers, final String body) throws Exception {
    EventStatus result;
    dynatrace.add(body, null);
    result = EventStatus.OK;
    return result;
  }

  @Override
  public ProcessStatus processEnd(final DynatraceSinkContext context) throws Exception {
    GenericSink.logger.debug("Closing dynatrace bulk");
    dynatrace.close();
    final int queueSize = dynatrace.getQueueSize();
    final ProcessStatus result;
    if (queueSize > bakeoffLimit) {
      result = ProcessStatus.BAKEOFF;
    }
    else {
      result = ProcessStatus.COMMIT;
    }
    return result;
  }

}
