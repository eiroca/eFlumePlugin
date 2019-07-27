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
package net.eiroca.sysadm.flume.core.eventDecoders;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IEventDecoder;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.eventdecoder.FormattedDecoder;
import net.eiroca.sysadm.flume.type.eventdecoder.JSONDecoder;
import net.eiroca.sysadm.flume.type.eventdecoder.RemappedJSONDecoder;
import net.eiroca.sysadm.flume.type.eventdecoder.StringDecoder;
import net.eiroca.sysadm.flume.type.eventdecoder.ext.DTCSVDecoder;
import net.eiroca.sysadm.flume.type.eventdecoder.ext.DTJSONDecoder;

public class EventDecoders {

  public static final Registry registry = new Registry();

  static {
    EventDecoders.registry.addEntry(StringDecoder.class.getName());
    EventDecoders.registry.addEntry("dt-csv", DTCSVDecoder.class.getName());
    EventDecoders.registry.addEntry("dt-json", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("dt", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("dynatrace", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("purepath", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("businesstransaction", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("bt", DTJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("string", StringDecoder.class.getName());
    EventDecoders.registry.addEntry("format", FormattedDecoder.class.getName());
    EventDecoders.registry.addEntry("macro", FormattedDecoder.class.getName());
    EventDecoders.registry.addEntry("formatted", FormattedDecoder.class.getName());
    EventDecoders.registry.addEntry("json", JSONDecoder.class.getName());
    EventDecoders.registry.addEntry("elastic", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("remapped", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("remappedjson", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("remapped-json", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("mapped", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("mappedjson", RemappedJSONDecoder.class.getName());
    EventDecoders.registry.addEntry("mapped-json", RemappedJSONDecoder.class.getName());
  }

  public static IEventDecoder<?> build(final String type, final ImmutableMap<String, String> config, final String prefix) {
    return (IEventDecoder<?>)FlumeHelper.buildIConfigurable(EventDecoders.registry.className(type), config, prefix);
  }

}
