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
package net.eiroca.sysadm.flume.type.action;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.sysadm.flume.core.actions.Action;

public class ActionDelete extends Action {

  final private transient ListParameter pHeaders = new ListParameter(params, "header-names", null);

  protected String[] headerNames;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    headerNames = pHeaders.get();
  }

  @Override
  public void run(final Map<String, String> headers, final String body) {
    if (headerNames != null) {
      for (String header : headerNames) {
        headers.remove(header);
      }
    }
    else {
      headers.remove(name);
    }
  }

}
