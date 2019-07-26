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
package net.eiroca.sysadm.flume.type.action;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.HeaderAction;

public class HeaderSet extends HeaderAction {

  final private transient StringParameter pValue = new StringParameter(params, "value");

  protected String value;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    value = pValue.get();
    if (value == null) {
      value = config.get(prefix);
    }
  }

  @Override
  public String getValue(final Map<String, String> headers, final String body) {
    return value;
  }

}
