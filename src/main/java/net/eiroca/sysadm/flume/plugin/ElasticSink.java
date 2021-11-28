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
package net.eiroca.sysadm.flume.plugin;

import java.util.Base64;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import net.eiroca.ext.library.elastic.ElasticBulk;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.core.util.GenericSink;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.util.context.ElasticSinkContext;

public class ElasticSink extends GenericSink<ElasticSinkContext> {

  /** Server URL */
  final StringParameter pEndPoint = new StringParameter(params, "server", null, true, false);
  final StringParameter pUsername = new StringParameter(params, "username", null);
  final StringParameter pPassword = new StringParameter(params, "password", null);
  final StringParameter pIndex = new StringParameter(params, "elastic-index", null, true, false);
  final BooleanParameter pUseEventTime = new BooleanParameter(params, "use-event-time", false);
  final StringParameter pType = new StringParameter(params, "elastic-type", "flume");
  final IntegerParameter pVersion = new IntegerParameter(params, "elastic-version", 7);
  final StringParameter pID = new StringParameter(params, "elastic-id", null);
  final StringParameter pPipeline = new StringParameter(params, "elastic-pipeline", null);
  final LongParameter pDiscardTime = new LongParameter(params, "elastic-overload-discard-time", 500);
  final IntegerParameter pNumThread = new IntegerParameter(params, "elastic-max-threads", 25);
  final IntegerParameter pBulkSize = new IntegerParameter(params, "bulk-size", 1 * 1024 * 1024);
  final BooleanParameter pCheckBulk = new BooleanParameter(params, "check-result", false);
  final IntegerParameter pQueueLimit = new IntegerParameter(params, "queue-limit", 100);
  final IntegerParameter pBakeOffLimit = new IntegerParameter(params, "bakeoff-limit", -1);

  /** Elastic Ingest URL to send events to. */
  String endPoint;
  String auth;
  String index;
  String type;
  String id;
  String pipeline;
  int queueLimit;
  int bakeoffLimit;
  boolean useEventTime;

  ElasticBulk elastic;

  @Override
  public void configure(final Context context) {
    super.configure(context);
    endPoint = pEndPoint.get();
    final String username = pUsername.get();
    final String password = pPassword.get();
    String auth = null;
    if (username != null) {
      final String credential = username + ":" + password;
      auth = Base64.getEncoder().encodeToString(credential.getBytes());
    }
    index = pIndex.get();
    type = pType.get();
    id = pID.get();
    if (LibStr.isEmptyOrNull(id)) {
      id = null;
    }
    useEventTime = pUseEventTime.get();
    pipeline = pPipeline.get();
    final int bulkSize = pBulkSize.get();
    final int threads = pNumThread.get();
    final int version = pVersion.get();
    elastic = new ElasticBulk(endPoint, version, pCheckBulk.get(), bulkSize, threads);
    if (auth != null) {
      elastic.setAuthorization("Basic " + auth);
    }
    queueLimit = pQueueLimit.get();
    if (queueLimit < 0) {
      queueLimit = 0;
    }
    bakeoffLimit = pBakeOffLimit.get();
    if (bakeoffLimit < 0) {
      bakeoffLimit = Integer.MAX_VALUE;
    }
    elastic.setDiscarTime(pDiscardTime.get());
  }

  @Override
  public ElasticSinkContext processBegin() throws Exception {
    final ElasticSinkContext context = new ElasticSinkContext(this);
    elastic.open();
    final int queueSize = (queueLimit > 0) ? elastic.getQueueSize() : 0;
    if (queueSize > queueLimit) {
      GenericSink.logger.error("Indexer overload {} - discarding events", queueSize);
      context.discard = true;
    }
    else if (queueSize > 0) {
      GenericSink.logger.info("Indexer overload {}", queueSize);
    }
    if (elastic.isOverload()) {
      GenericSink.logger.error("ElasticSearch Cluster overload - discarding events");

    }
    return context;
  }

  @Override
  protected boolean discard(final ElasticSinkContext context) {
    return context.discard;
  }

  @Override
  protected EventStatus process(final ElasticSinkContext context, final Event event, final Map<String, String> headers, final String body) throws Exception {
    EventStatus result;
    final String _index = MacroExpander.expand(index, headers, body, null, null, false, 0, 0, !useEventTime);
    final String _type = MacroExpander.expand(type, headers, body);
    final String _id = (id != null) ? MacroExpander.expand(id, headers, body) : null;
    final String _pipeline = (pipeline != null) ? MacroExpander.expand(pipeline, headers, body) : null;
    elastic.add(_index, _type, _id, _pipeline, body);
    result = EventStatus.OK;
    return result;
  }

  @Override
  public ProcessStatus processEnd(final ElasticSinkContext context) throws Exception {
    GenericSink.logger.debug("Closing elastic bulk");
    elastic.close();
    final int queueSize = elastic.getQueueSize();
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
