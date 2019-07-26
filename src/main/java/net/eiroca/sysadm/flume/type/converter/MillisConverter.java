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
package net.eiroca.sysadm.flume.type.converter;

import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IConverter;
import net.eiroca.sysadm.flume.api.IConverterResult;
import net.eiroca.sysadm.flume.core.ConverterResult;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

/**
 * Converter that converts the passed in value into milliseconds using the specified formatting
 * pattern
 */
public class MillisConverter extends ConfigurableObject implements IConverter<Long> {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pFormat = new StringParameter(params, "format", null);
  final private transient StringParameter pTimeZone = new StringParameter(params, "timezone", null);
  final private transient BooleanParameter pSilentError = new BooleanParameter(params, "silent-error", false);

  private static final String CFG_PATTERN_BACKUP = "format-";

  protected final List<DateTimeFormatter> formatters = new ArrayList<>();
  protected boolean silentError = false;
  protected DateTimeZone timezone = null;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    String pattern = pFormat.get();
    if (pattern != null) {
      formatters.add(DateTimeFormat.forPattern(pattern));
    }
    for (int i = 0; i < 10; i++) {
      pattern = config.get(LibStr.concatenate(prefix, MillisConverter.CFG_PATTERN_BACKUP, i));
      if (pattern != null) {
        formatters.add(DateTimeFormat.forPattern(pattern));
      }
    }
    Preconditions.checkArgument(formatters.size() > 0, "Must configure with a valid pattern");
    final String timezoneName = pTimeZone.get();
    if (timezoneName != null) {
      timezone = DateTimeZone.forID(timezoneName);
    }
    silentError = pSilentError.get();
  }

  @Override
  public IConverterResult<Long> convert(final String value) {
    final ConverterResult<Long> result = new ConverterResult<>();
    DateTime datetime = null;
    if (LibStr.isNotEmptyOrNull(value)) {
      for (final DateTimeFormatter formatter : formatters) {
        try {
          datetime = formatter.parseDateTime(value);
        }
        catch (final Exception e) {
          result.valid = false;
          result.error = e;
        }
        if (datetime != null) {
          result.valid = true;
          result.error = null;
          break;
        }
      }
      if ((timezone != null) && (datetime != null)) {
        final DateTime dt1 = new DateTime(datetime.getMillis());
        datetime = datetime.withZoneRetainFields(timezone);
        final DateTime dt2 = new DateTime(datetime.getMillis());
        MillisConverter.logger.trace("Changed datetime {} -> {}", dt1, dt2);
      }
    }
    if ((datetime == null) && (silentError)) {
      datetime = new DateTime();
    }
    if (datetime != null) {
      result.value = datetime.getMillis();
      result.valid = true;
    }
    return result;
  }

}
