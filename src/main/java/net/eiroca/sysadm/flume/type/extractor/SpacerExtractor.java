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

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.core.LibParser;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.extractors.Extractor;

public class SpacerExtractor extends Extractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient ListParameter pFields = new ListParameter(params, "field-names", new String[] {
      "message"
  });

  final private transient ListParameter pFieldsAlt = new ListParameter(params, "field-names-alt", null);

  public String[] fields;
  public String[] fieldsAlt;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    SpacerExtractor.logger.trace("config {}: {}", prefix, config);
    fields = pFields.get();
    fieldsAlt = pFieldsAlt.get();
  }

  @Override
  public List<String> getNames() {
    final List<String> result = new ArrayList<>();
    LibStr.addAll(result, fields);
    return result;
  }

  @Override
  public List<String> getAltNames() {
    List<String> result = null;
    if ((fieldsAlt != null) && (fieldsAlt.length > 0)) {
      result = new ArrayList<>();
      LibStr.addAll(result, fieldsAlt);
    }
    return result;
  }

  @Override
  public List<String> getValues(final String value) {
    if ((fields == null) || (value == null)) { return null; }
    SpacerExtractor.logger.debug("Source: {}", value);
    final List<String> result = LibParser.splitWithSpaces(value, -1);
    return result;
  }

}
