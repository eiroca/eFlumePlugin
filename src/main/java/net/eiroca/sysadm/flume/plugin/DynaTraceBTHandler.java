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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransaction;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransactions;
import com.google.protobuf.InvalidProtocolBufferException;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.api.IEventDecoder;
import net.eiroca.sysadm.flume.api.ext.IDynaTraceEventDecoder;
import net.eiroca.sysadm.flume.core.EventDecoders;
import net.eiroca.sysadm.flume.core.util.Flume;

public class DynaTraceBTHandler implements HTTPSourceHandler {

  protected static final Logger logger = LoggerFactory.getLogger(DynaTraceBTHandler.class);

  final Parameters params = new Parameters();
  final StringParameter pbtTypeHeader = new StringParameter(params, "btType-header", "btType");
  final StringParameter pbtNameHeader = new StringParameter(params, "btName-header", "btName");
  final StringParameter pSystemProfileHeader = new StringParameter(params, "systemProfile-header", "dtSystemProfile");
  final StringParameter pApplicationHeader = new StringParameter(params, "application-header", "dtApplication");
  final StringParameter pServerHeader = new StringParameter(params, "server-header", IDynaTraceEventDecoder.DEFAULT_SERVERHEADER);

  final StringParameter pDecoder = new StringParameter(params, "decoder", null);

  private String dtServerHeader;
  private String dtSystemProfileHeader;
  private String dtApplicationHeader;

  private String btNameHeader;
  private String btTypeHeader;

  private IEventDecoder<?> decoder;

  /**
   * Does nothing.
   */
  @Override
  public void configure(final Context context) {
    Flume.laodConfig(params, context);
    dtServerHeader = pServerHeader.get();
    dtSystemProfileHeader = pSystemProfileHeader.get();
    dtApplicationHeader = pApplicationHeader.get();
    btNameHeader = pbtNameHeader.get();
    btTypeHeader = pbtTypeHeader.get();
    final String decoderName = pDecoder.get();
    if (decoderName != null) {
      decoder = EventDecoders.build(decoderName, context.getParameters(), null);
      if (!(decoder instanceof IDynaTraceEventDecoder)) {
        DynaTraceBTHandler.logger.debug("Decoder '{}' is not valid.", EventDecoders.registry.className(decoderName));
        decoder = null;
      }
    }
    else {
      decoder = null;
    }
    DynaTraceBTHandler.logger.debug("{} decoder: {}", getClass().getSimpleName(), decoder);
  }

  /**
   * Creates an Event for each BusinessTransaction that's included in the request data and returns
   * them as a List.
   *
   * @param request
   * @return A {@link List} of {@link Event}s. Never returns <code>null</code>
   */
  @Override
  public List<Event> getEvents(final HttpServletRequest request) throws HTTPBadRequestException, IOException {
    final int length = request.getContentLength();
    final byte[] data = new byte[length];
    int read = 0;
    int total = 0;
    while ((read >= 0) && (total < length)) {
      read = request.getInputStream().read(data, total, length - total);
      if (read > 0) {
        total += read;
      }
    }
    try {
      final BusinessTransactions bts = BusinessTransactions.parseFrom(data);
      final List<Event> events = new ArrayList<>(bts.getBusinessTransactionsCount());
      for (final BusinessTransaction bt : bts.getBusinessTransactionsList()) {
        final Event event = EventBuilder.withBody(bt.toByteArray());
        final Map<String, String> headers = new TreeMap<>();
        if (LibStr.isNotEmptyOrNull(dtServerHeader) && bts.hasServerName()) {
          headers.put(dtServerHeader, bts.getServerName());
        }
        if (LibStr.isNotEmptyOrNull(dtSystemProfileHeader) && bt.hasSystemProfile()) {
          headers.put(dtSystemProfileHeader, bt.getSystemProfile());
        }
        if (LibStr.isNotEmptyOrNull(dtApplicationHeader) && bt.hasApplication()) {
          headers.put(dtApplicationHeader, bt.getApplication());
        }
        if (LibStr.isNotEmptyOrNull(btNameHeader) && bt.hasName()) {
          headers.put(btNameHeader, bt.getName());
        }
        if (LibStr.isNotEmptyOrNull(btTypeHeader) && bt.hasType()) {
          headers.put(btTypeHeader, bt.getType().name());
        }
        event.setHeaders(headers);
        if (decoder != null) {
          final List<?> decodedEvents = (List<?>)decoder.decode(event);
          for (final Object o : decodedEvents) {
            final String body = String.valueOf(o);
            final Event newEvent = EventBuilder.withBody(body.getBytes(), event.getHeaders());
            events.add(newEvent);
            DynaTraceBTHandler.logger.trace("Decoded: {}", newEvent);
          }
        }
        else {
          events.add(event);
          DynaTraceBTHandler.logger.trace("Added: {}", event);
        }
      }
      return events;
    }
    catch (final InvalidProtocolBufferException ipbe) {
      throw new HTTPBadRequestException(ipbe);
    }
  }

}
