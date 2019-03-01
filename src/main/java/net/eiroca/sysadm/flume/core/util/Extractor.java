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
package net.eiroca.sysadm.flume.core.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.eiroca.sysadm.flume.api.IExtractor;

abstract public class Extractor extends ConfigurableObject implements IExtractor {

  @Override
  public boolean hasNames() {
    return getNames() != null;
  }

  @Override
  public Map<String, String> getFields(final String value) {
    final List<String> names = getNames();
    final List<String> values = getValues(value);
    Map<String, String> result = null;
    if ((names != null) && (values != null)) {
      result = new HashMap<>(names.size() * 2);
      for (int i = 0; i < names.size(); i++) {
        final String key = names.get(i);
        final String val = values.get(i);
        result.put(key, val);
      }
    }
    else if (values != null) {
      result = new HashMap<>(values.size() * 2);
      for (int i = 0; i < values.size(); i++) {
        final String key = String.valueOf(i);
        final String val = values.get(i);
        result.put(key, val);
      }
    }
    return result;
  }

}
