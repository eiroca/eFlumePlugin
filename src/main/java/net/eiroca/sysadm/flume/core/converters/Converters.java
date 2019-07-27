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
package net.eiroca.sysadm.flume.core.converters;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IConverter;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.converter.CopyConverter;
import net.eiroca.sysadm.flume.type.converter.DoubleConverter;
import net.eiroca.sysadm.flume.type.converter.FormatConverter;
import net.eiroca.sysadm.flume.type.converter.LookupConverter;
import net.eiroca.sysadm.flume.type.converter.LowerConverter;
import net.eiroca.sysadm.flume.type.converter.MillisConverter;
import net.eiroca.sysadm.flume.type.converter.NumberConverter;
import net.eiroca.sysadm.flume.type.converter.PriorityConverter;
import net.eiroca.sysadm.flume.type.converter.StaticConverter;
import net.eiroca.sysadm.flume.type.converter.StringConverter;
import net.eiroca.sysadm.flume.type.converter.UpperConverter;

public class Converters {

  public static final Registry registry = new Registry();

  static {
    Converters.registry.addEntry(CopyConverter.class.getName());
    Converters.registry.addEntry("copy", CopyConverter.class.getName());
    Converters.registry.addEntry("set", StaticConverter.class.getName());
    Converters.registry.addEntry("static", StaticConverter.class.getName());
    Converters.registry.addEntry("date", MillisConverter.class.getName());
    Converters.registry.addEntry("datetime", MillisConverter.class.getName());
    Converters.registry.addEntry("default", CopyConverter.class.getName());
    Converters.registry.addEntry("double", DoubleConverter.class.getName());
    Converters.registry.addEntry("elapsed", DoubleConverter.class.getName());
    Converters.registry.addEntry("float", DoubleConverter.class.getName());
    Converters.registry.addEntry("id", NumberConverter.class.getName());
    Converters.registry.addEntry("integer", NumberConverter.class.getName());
    Converters.registry.addEntry("long", DoubleConverter.class.getName());
    Converters.registry.addEntry("lower", LowerConverter.class.getName());
    Converters.registry.addEntry("lowercase", LowerConverter.class.getName());
    Converters.registry.addEntry("millis", MillisConverter.class.getName());
    Converters.registry.addEntry("num", NumberConverter.class.getName());
    Converters.registry.addEntry("numeric", NumberConverter.class.getName());
    Converters.registry.addEntry("number", NumberConverter.class.getName());
    Converters.registry.addEntry("priority", PriorityConverter.class.getName());
    Converters.registry.addEntry("lookup", LookupConverter.class.getName());
    Converters.registry.addEntry("mapping", LookupConverter.class.getName());
    Converters.registry.addEntry("remapping", LookupConverter.class.getName());
    Converters.registry.addEntry("remap", LookupConverter.class.getName());
    Converters.registry.addEntry("time", MillisConverter.class.getName());
    Converters.registry.addEntry("timestamp", MillisConverter.class.getName());
    Converters.registry.addEntry("tolower", LowerConverter.class.getName());
    Converters.registry.addEntry("toupper", UpperConverter.class.getName());
    Converters.registry.addEntry("upper", UpperConverter.class.getName());
    Converters.registry.addEntry("uppercase", UpperConverter.class.getName());
    Converters.registry.addEntry("string", StringConverter.class.getName());
    Converters.registry.addEntry("format", FormatConverter.class.getName());
    Converters.registry.addEntry("formatted", FormatConverter.class.getName());
  }

  public static IConverter<?> build(final String name, final ImmutableMap<String, String> config, final String prefix) {
    return (IConverter<?>)FlumeHelper.buildIConfigurable(Converters.registry.className(name), config, prefix);
  }

}
