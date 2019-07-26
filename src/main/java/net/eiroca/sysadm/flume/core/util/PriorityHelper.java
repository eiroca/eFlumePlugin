/**
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
package net.eiroca.sysadm.flume.core.util;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import net.eiroca.library.core.Helper;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;

public class PriorityHelper {

  private static final String VALUE_SEP = "=";
  private static final String LIST_SEP = ",";

  protected static final Logger logger = Logs.getLogger();

  public static final int DEFAULT_PRIORITY = 3;
  public static final String DEFAULT_PRIORITY_MAPPING = "none=0,off=0,trace=1,finer=1,debug=2,fine=2,info=3,notification=3,warn=4,warning=4,error=5,severe=5,panic=5,critical=5,fatal=5,all=5,T=1,D=2,I=3,W=4,E=5,F=5";

  public String source = null;
  public Map<String, Integer> mappings = new HashMap<>();
  public int priorityDefault = 3;
  public int priorityMinimum = 0;
  public int priorityMaximum = 9;

  public int getPriority(final Map<String, String> headers, final String body) {
    if (source == null) { return priorityDefault; }
    final String _priorityName = MacroExpander.expand(source, headers, body);
    if (LibStr.isEmptyOrNull(_priorityName)) { return priorityDefault; }
    final Integer _priority = mappings.get(_priorityName.toLowerCase());
    if (_priority != null) { return _priority; }
    return Helper.getInt(_priorityName, priorityDefault);
  }

  public boolean isEnabled(final Map<String, String> headers, final String body) {
    final int priority = getPriority(headers, body);
    return (priority >= priorityMinimum) && (priority <= priorityMaximum);
  }

  public boolean isEnabled(final int priority) {
    return (priority >= priorityMinimum) && (priority <= priorityMaximum);
  }

  public void setPriorityMapping(final String mapping) {
    mappings = PriorityHelper.parsePriorityMapping(mapping);
  }

  public static HashMap<String, Integer> parsePriorityMapping(final String mapping) {
    final HashMap<String, Integer> map = new HashMap<>();
    try {
      for (final String mapEntry : mapping.split(PriorityHelper.LIST_SEP)) {
        final String[] valPair = mapEntry.split(PriorityHelper.VALUE_SEP);
        if (valPair.length == 2) {
          final String name = valPair[0].toLowerCase();
          final int val = Integer.parseInt(valPair[1]);
          map.put(name, val);
        }
      }
    }
    catch (final Exception e) {
      PriorityHelper.logger.error("Invalid priority mapping string {}", mapping, e);
    }
    return map;
  }

}
