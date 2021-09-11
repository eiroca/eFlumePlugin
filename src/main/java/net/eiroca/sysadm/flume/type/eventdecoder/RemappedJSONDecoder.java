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
package net.eiroca.sysadm.flume.type.eventdecoder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.flume.Event;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import net.eiroca.ext.library.gson.GsonCursor;
import net.eiroca.ext.library.gson.SimpleGson;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.eventDecoders.EventDecoder;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;

public class RemappedJSONDecoder extends EventDecoder<JsonObject> {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pEncoding = new StringParameter(params, "encoding", "UTF-8");
  final private transient BooleanParameter pExpandName = new BooleanParameter(params, "expand-name", true);
  final private transient StringParameter pBodyName = new StringParameter(params, "body-property", "message", false, true);
  final private transient BooleanParameter pIgnoreMissing = new BooleanParameter(params, "ignore-missing", false);
  final private transient StringParameter pMissingPrefix = new StringParameter(params, "missing-prefix", null);
  final private transient StringParameter pMappingFile = new StringParameter(params, "mapping", null);

  public String encoding;

  Map<String, String> mappedName = new HashMap<>();
  boolean expandName;
  String bodyName;
  String missingPrefix;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    encoding = pEncoding.get();
    expandName = pExpandName.get();
    bodyName = pBodyName.get();
    if (!pIgnoreMissing.get()) {
      missingPrefix = pMissingPrefix.get();
    }
    else {
      missingPrefix = null;
    }
    final String path = pMappingFile.get();
    mappedName.clear();
    if (path != null) {
      final Properties p = new Properties();
      InputStream is = null;
      try {
        is = new FileInputStream(path);
        p.load(is);
      }
      catch (final FileNotFoundException e) {
        RemappedJSONDecoder.logger.warn("Missing file " + path, e);
      }
      catch (final IOException e) {
        RemappedJSONDecoder.logger.warn("Error reading file " + path, e);
      }
      finally {
        Helper.close(is);
      }
      for (final Entry<Object, Object> mapping : p.entrySet()) {
        mappedName.put(String.valueOf(mapping.getKey()), String.valueOf(mapping.getValue()));
      }
    }
    RemappedJSONDecoder.logger.debug("RemappedJSON: " + this);
  }

  @Override
  public JsonObject decode(final Event event) {
    final SimpleGson data = new SimpleGson(expandName);
    final GsonCursor json = new GsonCursor(data);
    final Map<String, String> headers = event.getHeaders();
    final String message = bodyName != null ? FlumeHelper.getBody(event, encoding) : null;
    for (final Entry<String, String> header : headers.entrySet()) {
      final String value = header.getValue();
      if (LibStr.isEmptyOrNull(value)) {
        continue;
      }
      final String name = header.getKey();
      final boolean exist = mappedName.containsKey(name);
      String newName = mappedName.get(name);
      if (!exist && (missingPrefix != null)) {
        newName = missingPrefix + name;
        mappedName.put(name, newName);
      }
      if (LibStr.isNotEmptyOrNull(newName)) {
        json.addProperty(newName, value);
      }
    }
    if (LibStr.isNotEmptyOrNull(bodyName)) {
      json.addProperty(bodyName, message);
    }
    return data.getRoot();
  }

}
