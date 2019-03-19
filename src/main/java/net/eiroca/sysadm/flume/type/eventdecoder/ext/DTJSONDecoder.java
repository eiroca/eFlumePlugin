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
package net.eiroca.sysadm.flume.type.eventdecoder.ext;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.flume.Event;
import org.slf4j.Logger;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BtOccurrence;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransaction;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.protobuf.InvalidProtocolBufferException;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IDynaTraceEventDecoder;
import net.eiroca.sysadm.flume.core.util.EventDecoder;

public class DTJSONDecoder extends EventDecoder<List<JsonObject>> implements IDynaTraceEventDecoder<JsonObject> {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pServerHeader = new StringParameter(params, "server-header", IDynaTraceEventDecoder.DEFAULT_SERVERHEADER);
  final private transient BooleanParameter pForceDimensions = new BooleanParameter(params, "force-dimensions", true);
  final private transient BooleanParameter pForceMeasueres = new BooleanParameter(params, "force-measueres", true);

  private final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SZ");

  private String serverHeader;
  private boolean forceDimensions;
  private boolean forceMeasueres;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    serverHeader = pServerHeader.get();
    forceDimensions = pForceDimensions.get();
    forceMeasueres = pForceMeasueres.get();
  }

  @Override
  public List<JsonObject> decode(final Event event) {
    final List<JsonObject> events = new ArrayList<>();
    BusinessTransaction bt;
    try {
      bt = BusinessTransaction.parseFrom(event.getBody());
    }
    catch (final InvalidProtocolBufferException e) {
      DTJSONDecoder.logger.info("Invalid Business Transaction");
      return events;
    }
    if (!bt.hasType()) {
      DTJSONDecoder.logger.warn("Skipping serialization of event without type: btName='" + bt.getName() + "'");
      return events;
    }
    for (final BtOccurrence occurrence : bt.getOccurrencesList()) {
      final JsonObject jsonElement = new JsonObject();
      final String now = dateFormat.format(new Date());
      setProperty(jsonElement, "server", event.getHeaders().get(serverHeader), "");
      setProperty(jsonElement, "systemProfile", bt.hasSystemProfile() ? bt.getSystemProfile() : null, "");
      setProperty(jsonElement, "name", bt.hasName() ? bt.getName() : null, "");
      setProperty(jsonElement, "application", bt.hasApplication() ? bt.getApplication() : null, "");
      setProperty(jsonElement, "type", bt.hasType() ? bt.getType().name() : null, "PUREPATH");
      setProperty(jsonElement, "visitId", occurrence.hasVisitId() ? occurrence.getVisitId() : null, "");
      setProperty(jsonElement, "purePathId", occurrence.hasPurePathId() ? occurrence.getPurePathId() : null, "");
      setProperty(jsonElement, "startTime", occurrence.hasStartTime() ? dateFormat.format(new Date(occurrence.getStartTime())) : null, now);
      setProperty(jsonElement, "endTime", occurrence.hasEndTime() ? dateFormat.format(new Date(occurrence.getEndTime())) : null, now);
      final int nrOfSplittings = bt.getDimensionNamesCount();
      if ((nrOfSplittings > 0) || forceDimensions) {
        final JsonObject dimensions = new JsonObject();
        // safety net, in case the number of dimensions changed in between
        final int realSize = occurrence.getDimensionsCount();
        for (int i = 0; (i < nrOfSplittings) && (i < realSize); i++) {
          dimensions.addProperty(bt.getDimensionNames(i), occurrence.getDimensions(i));
        }
        jsonElement.add("dimensions", dimensions);
      }
      final int nrOfMeasures = bt.getMeasureNamesCount();
      if ((nrOfMeasures > 0) || forceMeasueres) {
        final JsonObject measures = new JsonObject();
        // safety net, in case the number of measures changed in between
        final int realSize = occurrence.getValuesCount();
        for (int i = 0; (i < nrOfMeasures) && (i < realSize); i++) {
          measures.addProperty(bt.getMeasureNames(i), occurrence.getValues(i));
        }
        jsonElement.add("measures", measures);
      }
      setProperty(jsonElement, "duration", occurrence.hasDuration() ? occurrence.getDuration() : null, "0");
      addProperty(jsonElement, "failed", occurrence.hasFailed() ? occurrence.getFailed() : null);
      addProperty(jsonElement, "actionName", occurrence.hasActionName() ? occurrence.getActionName() : null);
      addProperty(jsonElement, "apdex", occurrence.hasApdex() ? occurrence.getApdex() : null);
      addProperty(jsonElement, "converted", occurrence.hasConverted() ? occurrence.getConverted() : null);
      addProperty(jsonElement, "query", occurrence.hasQuery() ? occurrence.getQuery() : null);
      addProperty(jsonElement, "url", occurrence.hasUrl() ? occurrence.getUrl() : null);
      addProperty(jsonElement, "user", occurrence.hasUser() ? occurrence.getUser() : null);
      addProperty(jsonElement, "responseTime", occurrence.hasResponseTime() ? occurrence.getResponseTime() : null);
      addProperty(jsonElement, "cpuTime", occurrence.hasCpuTime() ? occurrence.getCpuTime() : null);
      addProperty(jsonElement, "execTime", occurrence.hasExecTime() ? occurrence.getExecTime() : null);
      addProperty(jsonElement, "suspensionTime", occurrence.hasSuspensionTime() ? occurrence.getSuspensionTime() : null);
      addProperty(jsonElement, "syncTime", occurrence.hasSyncTime() ? occurrence.getSyncTime() : null);
      addProperty(jsonElement, "waitTime", occurrence.hasWaitTime() ? occurrence.getWaitTime() : null);
      addProperty(jsonElement, "nrOfActions", occurrence.hasNrOfActions() ? occurrence.getNrOfActions() : null);
      addProperty(jsonElement, "clientFamily", occurrence.hasClientFamily() ? occurrence.getClientFamily() : null);
      addProperty(jsonElement, "clientIP", occurrence.hasClientIP() ? occurrence.getClientIP() : null);
      addProperty(jsonElement, "continent", occurrence.hasContinent() ? occurrence.getContinent() : null);
      addProperty(jsonElement, "country", occurrence.hasCountry() ? occurrence.getCountry() : null);
      addProperty(jsonElement, "city", occurrence.hasCity() ? occurrence.getCity() : null);
      addProperty(jsonElement, "failedActions", occurrence.hasFailedActions() ? occurrence.getFailedActions() : null);
      addProperty(jsonElement, "clientErrors", occurrence.hasClientErrors() ? occurrence.getClientErrors() : null);
      addProperty(jsonElement, "exitActionFailed", occurrence.hasExitActionFailed() ? occurrence.getExitActionFailed() : null);
      addProperty(jsonElement, "bounce", occurrence.hasBounce() ? occurrence.getBounce() : null);
      addProperty(jsonElement, "osFamily", occurrence.hasOsFamily() ? occurrence.getOsFamily() : null);
      addProperty(jsonElement, "osName", occurrence.hasOsName() ? occurrence.getOsName() : null);
      addProperty(jsonElement, "connectionType", occurrence.hasConnectionType() ? occurrence.getConnectionType() : null);
      final int nrOfConvertedBy = occurrence.getConvertedByCount();
      if (nrOfConvertedBy > 0) {
        final JsonArray convertedBy = new JsonArray();
        for (int i = 0; i < nrOfConvertedBy; i++) {
          convertedBy.add(new JsonPrimitive(occurrence.getConvertedBy(i)));
        }
        jsonElement.add("convertedBy", convertedBy);
      }
      events.add(jsonElement);
    }
    return events;
  }

  void addProperty(final JsonObject jsonElement, final String name, final Object value) {
    if (value != null) {
      jsonElement.addProperty(name, String.valueOf(value));
    }
  }

  void setProperty(final JsonObject jsonElement, final String name, final Object value, final String def) {
    jsonElement.addProperty(name, value != null ? String.valueOf(value) : def);
  }

}
