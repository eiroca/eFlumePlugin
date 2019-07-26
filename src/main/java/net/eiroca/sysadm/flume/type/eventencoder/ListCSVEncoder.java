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
package net.eiroca.sysadm.flume.type.eventencoder;

import java.util.List;
import org.apache.flume.Event;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.api.ext.IEventListEncoder;
import net.eiroca.sysadm.flume.core.util.EventEncoder;

public class ListCSVEncoder extends EventEncoder<List<Object>> implements IEventListEncoder<List<Object>> {

  final private transient StringParameter pSeparator = new StringParameter(params, "separator", ",");
  final private transient StringParameter pNullValue = new StringParameter(params, "null", "");

  private String sep;
  private String nullVal;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    sep = pSeparator.get();
    nullVal = pNullValue.get();
  }

  @Override
  public void encode(final List<Object> data) {
    if (data == null) { return; }
    final Event e = newEvent();
    e.setBody(LibStr.merge(data, sep, nullVal).getBytes());
    callback.notifyEvent(e);
  }

}
