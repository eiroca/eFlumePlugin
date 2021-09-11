/**
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
package net.eiroca.sysadm.flume.core.util;

import java.util.Calendar;
import java.util.TimeZone;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.tools.TimestampRoundDownUtil;
import org.slf4j.Logger;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.core.LibMap;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IConfigurable;
import net.eiroca.sysadm.flume.api.INamedObject;

public class FlumeHelper {

  transient private static final Logger logger = Logs.getLogger();

  public static final String BODY_ERROR_MESSAGE = "Unknow message of %d byte(s)";

  public static IConfigurable buildIConfigurable(final String clazzName, final ImmutableMap<String, String> config, final String prefix) {
    return FlumeHelper.buildIConfigurable(null, clazzName, config, prefix);
  }

  public static IConfigurable buildIConfigurable(String name, final String clazzName, final ImmutableMap<String, String> config, final String prefix) {
    IConfigurable obj = null;
    try {
      obj = (IConfigurable)Class.forName(clazzName).newInstance();
      if (name != null) {
        ((INamedObject)obj).setName(name);
      }
      if (obj.isConfigurable()) {
        name = (name == null) ? prefix : name;
        obj.configure(config, prefix);
        FlumeHelper.logger.trace("Built {}: {}", clazzName, obj);
      }
    }
    catch (final Exception e) {
      Throwables.propagate(e);
    }
    return obj;
  }

  public static Context getContext(final ImmutableMap<String, String> config, final String prefix) {
    return new Context(LibMap.getSubMap(config, prefix));
  }

  final public static String getBody(final Event event, final String encoding) {
    return LibStr.getMessage(event.getBody(), encoding, FlumeHelper.BODY_ERROR_MESSAGE);
  }

  final public static long roundDown(int roundDown, final int unit, final long ts, final TimeZone timeZone) {
    long timestamp = ts;
    if (roundDown <= 0) {
      roundDown = 1;
    }
    switch (unit) {
      case Calendar.SECOND:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampSeconds(ts, roundDown, timeZone);
        break;
      case Calendar.MINUTE:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampMinutes(ts, roundDown, timeZone);
        break;
      case Calendar.HOUR_OF_DAY:
        timestamp = TimestampRoundDownUtil.roundDownTimeStampHours(ts, roundDown, timeZone);
        break;
      default:
        timestamp = ts;
        break;
    }
    return timestamp;
  }

  public static void laodConfig(final Parameters params, final Context context) throws IllegalArgumentException {
    final ImmutableMap<String, String> config = context.getParameters();
    params.loadConfig(config, null);
  }

  public static void laodConfig(final Parameters params, final Context context, final String prefix) throws IllegalArgumentException {
    final ImmutableMap<String, String> config = context.getParameters();
    params.loadConfig(config, prefix);
  }

}
