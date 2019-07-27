/**
 *
 * Copyright (C) 1999-2019 Enrico Croce - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.type.eventdecoder.ext;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Event;
import org.slf4j.Logger;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BtOccurrence;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransaction;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.CharParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IDynaTraceEventDecoder;
import net.eiroca.sysadm.flume.core.eventDecoders.EventDecoder;

public class DTCSVDecoder extends EventDecoder<List<String>> implements IDynaTraceEventDecoder<String> {

  transient private static final Logger logger = Logs.getLogger();

  final StringParameter pServerHeader = new StringParameter(params, "server-header", IDynaTraceEventDecoder.DEFAULT_SERVERHEADER);
  final BooleanParameter pNumericDate = new BooleanParameter(params, "numericDate", false);
  final CharParameter pFieldDelimiter = new CharParameter(params, "field-delimiter", ';');
  final CharParameter pCollectionDelimiter = new CharParameter(params, "collection-delimiter", '|');
  final CharParameter pLineDelimiter = new CharParameter(params, "line-delimiter", '\n');
  final CharParameter pKeyMapDelimiter = new CharParameter(params, "keymap-delimiter", '=');
  final CharParameter pEscapeChar = new CharParameter(params, "escape-char", '\\');
  final CharParameter pFieldDelimiterEscaped = new CharParameter(params, "field-escaped", ';');
  final CharParameter pCollectionDelimiterEscaped = new CharParameter(params, "collection-escaped", '|');
  final CharParameter pLineDelimiterEscaped = new CharParameter(params, "line-escaped", 'n');
  final CharParameter pKeyMapDelimiterEscaped = new CharParameter(params, "keymap-escaped", '=');

  private String serverHeader;
  private boolean numericDate;
  private char fieldDelimiter;
  private char collectionDelimiter;
  private char lineDelimiter;
  private char keyMapDelimiter;
  private char escapeChar;
  private char fieldEscaped;
  private char collectionEscaped;
  private char lineEscaped;
  private char keyMapEscaped;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    serverHeader = pServerHeader.get();
    numericDate = pNumericDate.get();
    fieldDelimiter = pFieldDelimiter.get();
    collectionDelimiter = pCollectionDelimiter.get();
    lineDelimiter = pLineDelimiter.get();
    keyMapDelimiter = pKeyMapDelimiter.get();
    escapeChar = pEscapeChar.get();
    fieldEscaped = pFieldDelimiterEscaped.get();
    collectionEscaped = pCollectionDelimiterEscaped.get();
    lineEscaped = pLineDelimiterEscaped.get();
    keyMapEscaped = pKeyMapDelimiterEscaped.get();
    DTCSVDecoder.logger.info("Field Delimiter: {}", fieldDelimiter);
    DTCSVDecoder.logger.info("Collection Delimiter: {}", collectionDelimiter);
  }

  @Override
  public List<String> decode(final Event event) {
    final List<String> events = new ArrayList<>();
    BusinessTransaction bt;
    try {
      bt = BusinessTransaction.parseFrom(event.getBody());
    }
    catch (final InvalidProtocolBufferException e) {
      DTCSVDecoder.logger.info("Invalid Business Transaction");
      return events;
    }
    if (!bt.hasType()) {
      DTCSVDecoder.logger.warn("Skipping serialization of event without type: btName='" + bt.getName() + "'");
      return events;
    }
    final String server = event.getHeaders().get(serverHeader);
    if (bt.getType() == BusinessTransaction.Type.USER_ACTION) {
      processUserAction(events, server, bt);
    }
    else if (bt.getType() == BusinessTransaction.Type.PUREPATH) {
      processPurePath(events, server, bt);
    }
    else if (bt.getType() == BusinessTransaction.Type.VISIT) {
      processVisit(events, server, bt);
    }
    else {
      DTCSVDecoder.logger.warn("Skipping serialization of event of wrong type: '{}', btName: '{}'", bt.getType(), bt.getName());
    }
    return events;
  }

  void addColumn(final List<String> columns, final Object data, final boolean escape) {
    columns.add(data != null ? (escape ? escape(data.toString()).toString() : data.toString()) : "");
  }

  String toCSV(final List<String> columns) {
    final StringBuffer sb = new StringBuffer(256);
    for (final String s : columns) {
      sb.append(s);
      sb.append(fieldDelimiter);
    }
    sb.append(lineDelimiter);
    return sb.toString();
  }

  /**
   * Escapes <code>; = ,<code> using <code>\</code> as delimiter char. <code>\n</code> is escaped as
   * <code>\\n</code> rather than <code>\\\n</code> as Hive first reads the data and then does the
   * escaping.
   *
   * @param string - the {@link String} to be escaped
   * @return
   */
  StringBuffer escape(final String string) {
    final char[] chars = string.toCharArray();
    final StringBuffer result = new StringBuffer();
    for (final char c : chars) {
      if (c == lineDelimiter) {
        result.append(escapeChar).append(lineEscaped);
      }
      else if (c == fieldDelimiter) {
        result.append(escapeChar).append(fieldEscaped);
      }
      else if (c == collectionDelimiter) {
        result.append(escapeChar).append(collectionEscaped);
      }
      else if (c == keyMapDelimiter) {
        result.append(escapeChar).append(keyMapEscaped);
      }
      else {
        result.append(c);
      }
    }
    return result;
  }

  /**
   * Appends the given date to the given stringBuilder. Depending on the configuration it will be
   * appended as a number like "123456789.00" or as a JDBC compliant {@link String} like "2013-02-15
   * 10:33:03.226".
   *
   * @param stringBuilder
   * @param date
   * @see Timestamp
   */
  String appendDate(final long date) {
    final StringBuilder stringBuilder = new StringBuilder();
    if (numericDate) {
      stringBuilder.append(date);
      // insert a decimal point to make seconds out of the milliseconds
      stringBuilder.insert(stringBuilder.length() - 3, '.');
    }
    else {
      stringBuilder.append(new Timestamp(date));
    }
    return stringBuilder.toString();
  }

  String getSplittings(final BusinessTransaction bt, final BtOccurrence occurrence) {
    final StringBuilder sb = new StringBuilder(64);
    final int nrOfSplittings = bt.getDimensionNamesCount();
    for (int i = 0; i < nrOfSplittings; i++) {
      if (i > 0) {
        sb.append(collectionDelimiter);
      }
      sb.append(escape(bt.getDimensionNames(i))).append(keyMapDelimiter).append(occurrence.getDimensions(i));
    }
    return sb.toString();
  }

  String getMeasures(final BusinessTransaction bt, final BtOccurrence occurrence) {
    final StringBuilder sb = new StringBuilder(64);
    final int nrOfMeasures = bt.getMeasureNamesCount();
    for (int i = 0; i < nrOfMeasures; i++) {
      if (i > 0) {
        sb.append(collectionDelimiter);
      }
      sb.append(escape(bt.getMeasureNames(i))).append(keyMapDelimiter).append(occurrence.getValues(i));
    }
    return sb.toString();
  }

  String getConverted(final BusinessTransaction bt, final BtOccurrence occurrence) {
    final StringBuilder sb = new StringBuilder(64);
    final int convertedByCount = occurrence.getConvertedByCount();
    for (int i = 0; i < convertedByCount; i++) {
      if (i > 0) {
        sb.append(collectionDelimiter);
      }
      sb.append(escape(occurrence.getConvertedBy(i)));
    }
    return sb.toString();
  }

  public void processVisit(final List<String> events, final String server, final BusinessTransaction bt) {
    final List<String> columns = new ArrayList<>();
    for (final BtOccurrence occurrence : bt.getOccurrencesList()) {
      columns.clear();
      // Common
      addColumn(columns, server, true);
      addColumn(columns, bt.hasSystemProfile() ? bt.getSystemProfile() : null, true);
      addColumn(columns, bt.hasName() ? bt.getName() : null, true);
      addColumn(columns, bt.hasApplication() ? bt.getApplication() : null, true);
      addColumn(columns, bt.hasType() ? bt.getType() : null, true);
      addColumn(columns, occurrence.hasVisitId() ? occurrence.getVisitId() : null, true);
      addColumn(columns, occurrence.hasPurePathId() ? occurrence.getPurePathId() : null, true);
      addColumn(columns, occurrence.hasStartTime() ? appendDate(occurrence.getStartTime()) : null, true);
      addColumn(columns, occurrence.hasEndTime() ? appendDate(occurrence.getEndTime()) : null, true);
      addColumn(columns, getSplittings(bt, occurrence), false);
      addColumn(columns, getMeasures(bt, occurrence), false);
      addColumn(columns, occurrence.hasDuration() ? occurrence.getDuration() : null, true);
      // Visit
      addColumn(columns, occurrence.hasUser() ? occurrence.getUser() : null, true);
      addColumn(columns, occurrence.hasConverted() ? occurrence.getConverted() : null, true);
      addColumn(columns, occurrence.hasApdex() ? occurrence.getApdex() : null, true);
      addColumn(columns, occurrence.hasNrOfActions() ? occurrence.getNrOfActions() : null, true);
      addColumn(columns, occurrence.hasClientFamily() ? occurrence.getClientFamily() : null, true);
      addColumn(columns, occurrence.hasClientIP() ? occurrence.getClientIP() : null, true);
      addColumn(columns, occurrence.hasContinent() ? occurrence.getContinent() : null, true);
      addColumn(columns, occurrence.hasCountry() ? occurrence.getCountry() : null, true);
      addColumn(columns, occurrence.hasCity() ? occurrence.getCity() : null, true);
      addColumn(columns, occurrence.hasFailedActions() ? occurrence.getFailedActions() : null, true);
      addColumn(columns, occurrence.hasClientErrors() ? occurrence.getClientErrors() : null, true);
      addColumn(columns, occurrence.hasExitActionFailed() ? occurrence.getExitActionFailed() : null, true);
      addColumn(columns, occurrence.hasBounce() ? occurrence.getBounce() : null, true);
      addColumn(columns, occurrence.hasOsFamily() ? occurrence.getOsFamily() : null, true);
      addColumn(columns, occurrence.hasOsName() ? occurrence.getOsName() : null, true);
      addColumn(columns, occurrence.hasConnectionType() ? occurrence.getConnectionType() : null, true);
      addColumn(columns, getConverted(bt, occurrence), false);
      events.add(toCSV(columns));
    }
  }

  public void processUserAction(final List<String> events, final String server, final BusinessTransaction bt) {
    final List<String> columns = new ArrayList<>();
    for (final BtOccurrence occurrence : bt.getOccurrencesList()) {
      columns.clear();
      // Common
      addColumn(columns, server, true);
      addColumn(columns, bt.hasSystemProfile() ? bt.getSystemProfile() : null, true);
      addColumn(columns, bt.hasName() ? bt.getName() : null, true);
      addColumn(columns, bt.hasApplication() ? bt.getApplication() : null, true);
      addColumn(columns, bt.hasType() ? bt.getType() : null, true);
      addColumn(columns, occurrence.hasVisitId() ? occurrence.getVisitId() : null, true);
      addColumn(columns, occurrence.hasPurePathId() ? occurrence.getPurePathId() : null, true);
      addColumn(columns, occurrence.hasStartTime() ? occurrence.getStartTime() : null, true);
      addColumn(columns, occurrence.hasEndTime() ? occurrence.getEndTime() : null, true);
      addColumn(columns, getSplittings(bt, occurrence), false);
      addColumn(columns, getMeasures(bt, occurrence), false);
      addColumn(columns, occurrence.hasDuration() ? occurrence.getDuration() : null, true);
      // Page action detail data
      addColumn(columns, occurrence.hasFailed() ? occurrence.getFailed() : null, true);
      addColumn(columns, occurrence.hasActionName() ? occurrence.getActionName() : null, true);
      addColumn(columns, occurrence.hasUrl() ? occurrence.getUrl() : null, true);
      addColumn(columns, occurrence.hasResponseTime() ? occurrence.getResponseTime() : null, true);
      addColumn(columns, occurrence.hasCpuTime() ? occurrence.getCpuTime() : null, true);
      addColumn(columns, occurrence.hasExecTime() ? occurrence.getExecTime() : null, true);
      addColumn(columns, occurrence.hasSuspensionTime() ? occurrence.getSuspensionTime() : null, true);
      addColumn(columns, occurrence.hasSyncTime() ? occurrence.getSyncTime() : null, true);
      addColumn(columns, occurrence.hasWaitTime() ? occurrence.getWaitTime() : null, true);
      addColumn(columns, occurrence.hasClientErrors() ? occurrence.getClientErrors() : null, true);
      addColumn(columns, occurrence.hasClientTime() ? occurrence.getClientTime() : null, true);
      addColumn(columns, occurrence.hasNetworkTime() ? occurrence.getNetworkTime() : null, true);
      addColumn(columns, occurrence.hasServerTime() ? occurrence.getServerTime() : null, true);
      addColumn(columns, occurrence.hasUrlRedirectionTime() ? occurrence.getUrlRedirectionTime() : null, true);
      addColumn(columns, occurrence.hasDnsTime() ? occurrence.getDnsTime() : null, true);
      addColumn(columns, occurrence.hasConnectTime() ? occurrence.getConnectTime() : null, true);
      addColumn(columns, occurrence.hasSslTime() ? occurrence.getSslTime() : null, true);
      addColumn(columns, occurrence.hasDocumentRequestTime() ? occurrence.getDocumentRequestTime() : null, true);
      addColumn(columns, occurrence.hasDocumentResponseTime() ? occurrence.getDocumentResponseTime() : null, true);
      addColumn(columns, occurrence.hasProcessingTime() ? occurrence.getProcessingTime() : null, true);
      events.add(toCSV(columns));
    }
  }

  public void processPurePath(final List<String> events, final String server, final BusinessTransaction bt) {
    final List<String> columns = new ArrayList<>();
    for (final BtOccurrence occurrence : bt.getOccurrencesList()) {
      columns.clear();
      // Common
      addColumn(columns, server, true);
      addColumn(columns, bt.hasSystemProfile() ? bt.getSystemProfile() : null, true);
      addColumn(columns, bt.hasName() ? bt.getName() : null, true);
      addColumn(columns, bt.hasApplication() ? bt.getApplication() : null, true);
      addColumn(columns, bt.hasType() ? bt.getType() : null, true);
      addColumn(columns, occurrence.hasVisitId() ? occurrence.getVisitId() : null, true);
      addColumn(columns, occurrence.hasPurePathId() ? occurrence.getPurePathId() : null, true);
      addColumn(columns, occurrence.hasStartTime() ? occurrence.getStartTime() : null, true);
      addColumn(columns, occurrence.hasEndTime() ? occurrence.getEndTime() : null, true);
      addColumn(columns, getSplittings(bt, occurrence), false);
      addColumn(columns, getMeasures(bt, occurrence), false);
      addColumn(columns, occurrence.hasDuration() ? occurrence.getDuration() : null, true);
      // PurePath detail data
      addColumn(columns, occurrence.hasFailed() ? occurrence.getFailed() : null, true);
      addColumn(columns, occurrence.hasResponseTime() ? occurrence.getResponseTime() : null, true);
      addColumn(columns, occurrence.hasCpuTime() ? occurrence.getCpuTime() : null, true);
      addColumn(columns, occurrence.hasExecTime() ? occurrence.getExecTime() : null, true);
      addColumn(columns, occurrence.hasSuspensionTime() ? occurrence.getSuspensionTime() : null, true);
      addColumn(columns, occurrence.hasSyncTime() ? occurrence.getSyncTime() : null, true);
      addColumn(columns, occurrence.hasWaitTime() ? occurrence.getWaitTime() : null, true);
      events.add(toCSV(columns));
    }
  }

}
