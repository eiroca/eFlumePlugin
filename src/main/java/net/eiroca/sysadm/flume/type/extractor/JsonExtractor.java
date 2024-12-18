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
package net.eiroca.sysadm.flume.type.extractor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.ext.library.gson.LibGson;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

public class JsonExtractor extends ConfigurableObject implements IExtractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient BooleanParameter pExpandNodes = new BooleanParameter(params, "expand-nodes", true);
  final private transient BooleanParameter pIgnoreMissing = new BooleanParameter(params, "ignore-missing", false);
  final private transient StringParameter pMissingPrefix = new StringParameter(params, "missing-prefix", "");
  final private transient StringParameter pMappingFile = new StringParameter(params, "mapping", null);

  Map<String, String> mappedName = new HashMap<>();
  boolean expandNodes;
  String missingPrefix;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    expandNodes = pExpandNodes.get();
    if (!pIgnoreMissing.get()) {
      missingPrefix = pMissingPrefix.get();
    }
    else {
      missingPrefix = null;
    }
    final String path = pMappingFile.get();
    mappedName.clear();
    if (path != null) {
      try {
        final Properties p = Helper.loadProperties(path, false);
        for (final Entry<Object, Object> mapping : p.entrySet()) {
          mappedName.put(String.valueOf(mapping.getKey()), String.valueOf(mapping.getValue()));
        }
      }
      catch (final FileNotFoundException e) {
        logger.warn("Missing file " + path, e);
      }
      catch (final IOException e) {
        logger.warn("Error reading file " + path, e);
      }
    }
    logger.debug("JsonExtractor: " + this);
  }

  @Override
  public Tags getTags(final String value) {
    Tags t = LibGson.getTags(value, expandNodes, missingPrefix, mappedName);
    return t;
  }

}
