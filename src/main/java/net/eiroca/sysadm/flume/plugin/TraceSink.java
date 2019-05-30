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
package net.eiroca.sysadm.flume.plugin;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.api.IEventDecoder;
import net.eiroca.sysadm.flume.core.EventDecoders;
import net.eiroca.sysadm.flume.core.util.GenericSink;
import net.eiroca.sysadm.flume.core.util.GenericSinkContext;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

public class TraceSink extends GenericSink<GenericSinkContext<?>> {

  final private StringParameter pTraceLogger = new StringParameter(params, "logger", "%{logger}");
  final private StringParameter pTraceLoggerDefault = new StringParameter(params, "logger-default", "log.TraceSink");
  final private BooleanParameter pTraceHeader = new BooleanParameter(params, "log-header", false);
  final private StringParameter pDecoder = new StringParameter(params, "decoder", EventDecoders.registry.defaultName());
  final private StringParameter pTraceMessage = new StringParameter(params, "message", "%()");

  private String logName;
  private String defLogName;
  private boolean logHeader;
  private String logMessage;

  private IEventDecoder<?> decoder;
  private final HashMap<String, Logger> loggers = new HashMap<>();

  @Override
  public void configure(final Context context) {
    super.configure(context);
    logName = pTraceLogger.get();
    defLogName = pTraceLoggerDefault.get();
    logHeader = pTraceHeader.get();
    logMessage = pTraceMessage.get();
    decoder = EventDecoders.build(pDecoder.get(), context.getParameters(), null);
  }

  @Override
  public EventStatus processEvent(final GenericSinkContext<?> context, final Event event) throws Exception {
    GenericSink.logger.trace("Tracing {}", event);
    final Map<String, String> headers = event.getHeaders();
    final int priority = priorityHelper.getPriority(headers, null);
    if (priorityHelper.isEnabled(priority)) {
      final String body = String.valueOf(decoder.decode(event));
      writeLog(headers, body, priority);
    }
    return EventStatus.OK;
  }

  private final void writeLog(final Map<String, String> headers, final String body, int priority) {
    final Logger log = getLogger(headers, body);
    final String message = getMessage(headers, body);
    if (logHeader) {
      MDC.clear();
      try {
        for (final String key : headers.keySet()) {
          MDC.put(key, headers.get(key));
        }
      }
      catch (final ConcurrentModificationException e) {
        GenericSink.logger.warn("Headers incomplete: {}", headers, e);
      }
    }
    switch (priority) {
      case 0:
        break;
      case 1:
        log.trace(message);
        break;
      case 2:
        log.debug(message);
        break;
      case 3:
        log.info(message);
        break;
      case 4:
        log.warn(message);
        break;
      case 5:
        log.error(message);
        break;
      default:
        log.trace(message);
        break;
    }
  }

  public String getMessage(final Map<String, String> headers, final String body) {
    final String _message = MacroExpander.expand(logMessage, headers, body);
    return _message;
  }

  public Logger getLogger(final Map<String, String> headers, final String body) {
    String _loggerName = MacroExpander.expand(logName, headers, body);
    if (LibStr.isEmptyOrNull(_loggerName)) {
      _loggerName = defLogName;
    }
    Logger _logger;
    synchronized (loggers) {
      _logger = loggers.get(_loggerName);
      if (_logger == null) {
        _logger = LoggerFactory.getLogger(_loggerName);
        loggers.put(_loggerName, _logger);
      }
    }
    return _logger;
  }

}
