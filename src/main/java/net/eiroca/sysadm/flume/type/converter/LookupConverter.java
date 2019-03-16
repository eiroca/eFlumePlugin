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
package net.eiroca.sysadm.flume.type.converter;

import java.util.HashMap;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.Converter;

/**
 * Converter that simply returns the passed in value
 */
public class LookupConverter extends Converter<String> {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pDefault = new StringParameter(params, "default", "info");
  final private transient StringParameter pMapping = new StringParameter(params, "mapping", null);
  final private transient StringParameter pMappingMissing = new StringParameter(params, "mapping-mising", null);
  final private transient StringParameter pMappingSeparator = new StringParameter(params, "mapping-entry-separator", ",");
  final private transient StringParameter pMappingAssign = new StringParameter(params, "mapping-value-separator", "=");

  protected String defValue;
  protected String mappingDefault;
  protected HashMap<String, String> mapping;

  @Override
  public String doConvert(final String value) {
    String newVal = (value != null) ? value.toLowerCase().trim() : defValue;
    if (mapping != null) {
      final String mapVal = mapping.get(newVal);
      newVal = (mapVal != null) ? mapVal : (mappingDefault != null) ? mappingDefault : newVal;
    }
    return newVal;
  }

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    defValue = pDefault.get();
    final String mappingString = pMapping.get();
    mapping = parseMapping(mappingString);
    mappingDefault = pMappingMissing.get();
  }

  protected HashMap<String, String> parseMapping(final String mapping) {
    if (mapping == null) { return null; }
    final String entrySep = pMappingSeparator.get();
    final String valueSep = pMappingAssign.get();
    final HashMap<String, String> map = new HashMap<>();
    try {
      for (final String mapEntry : mapping.split(entrySep)) {
        final String[] valPair = mapEntry.split(valueSep);
        final String name = valPair[0].toLowerCase();
        final String val = valPair[1];
        map.put(name, val);
      }
    }
    catch (final Exception e) {
      LookupConverter.logger.error("Invalid priority mapping string {}", mapping, e);
    }
    return (map.size() > 0) ? map : null;
  }
}
