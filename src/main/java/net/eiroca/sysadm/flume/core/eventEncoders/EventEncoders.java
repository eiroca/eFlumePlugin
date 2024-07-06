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
package net.eiroca.sysadm.flume.core.eventEncoders;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IEventEncoder;
import net.eiroca.sysadm.flume.api.IEventNotify;
import net.eiroca.sysadm.flume.core.eventDecoders.EventDecoders;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.eventdecoder.StringDecoder;
import net.eiroca.sysadm.flume.type.eventencoder.ListCSVEncoder;

public class EventEncoders {

  public static final Registry<String> registry = new Registry<String>();

  static {
    EventEncoders.registry.addEntry(StringDecoder.class.getName());
    EventDecoders.registry.addEntry("csv", ListCSVEncoder.class.getName());
  }

  public static IEventEncoder<?> build(final String type, final ImmutableMap<String, String> config, final String prefix, final IEventNotify callback) {
    final IEventEncoder<?> encoder = (IEventEncoder<?>)FlumeHelper.buildIConfigurable(EventEncoders.registry.value(type), config, prefix);
    encoder.setCallBack(callback);
    return encoder;
  }

}
