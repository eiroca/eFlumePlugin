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
package net.eiroca.sysadm.flume.type.converter;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.api.IConverter;
import net.eiroca.sysadm.flume.api.IConverterResult;
import net.eiroca.sysadm.flume.core.converters.ConverterResult;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

/**
 * Converter that converts the passed in value into milliseconds using the specified formatting
 * pattern
 */
public class MillisConverter extends ConfigurableObject implements IConverter<Long> {

  final private transient StringParameter pFormat = new StringParameter(params, "format", null);
  final private transient StringParameter pLocale = new StringParameter(params, "locale", "en");
  final private transient StringParameter pTimeZone = new StringParameter(params, "timezone", null);
  final private transient BooleanParameter pSilentError = new BooleanParameter(params, "silent-error", true);

  private static final String CFG_PATTERN_BACKUP = "format-";

  protected final List<DateTimeFormatter> formatters = new ArrayList<>();
  protected boolean silentError = false;
  protected Locale locale = null;
  protected ZoneId zid;

  public static Locale stringToLocale(final String s) {
    final StringTokenizer tempStringTokenizer = new StringTokenizer(s, ",");
    if (tempStringTokenizer.hasMoreTokens()) {
      final String l = tempStringTokenizer.nextToken();
      if (tempStringTokenizer.hasMoreTokens()) {
        final String c = tempStringTokenizer.nextToken();
        return new Locale(l, c);
      }
      return new Locale(l);
    }
    return null;
  }

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    final String localeStr = pLocale.get();
    locale = MillisConverter.stringToLocale(localeStr);
    final String timezoneName = pTimeZone.get();
    if (timezoneName != null) {
      zid = ZoneId.of(timezoneName);
    }
    else {
      zid = ZoneId.systemDefault();
    }
    String pattern = pFormat.get();
    if (pattern != null) {
      formatters.add(DateTimeFormatter.ofPattern(pattern, locale).withZone(zid));
    }
    for (int i = 0; i < 10; i++) {
      pattern = config.get(LibStr.concatenate(prefix, MillisConverter.CFG_PATTERN_BACKUP, i));
      if (pattern != null) {
        formatters.add(DateTimeFormatter.ofPattern(pattern, locale).withZone(zid));
      }
    }
    Preconditions.checkArgument(formatters.size() > 0, "Must configure with a valid pattern");
    silentError = pSilentError.get();
  }

  @Override
  public IConverterResult<Long> convert(final String value) {
    final ConverterResult<Long> result = new ConverterResult<>();
    ZonedDateTime datetime = null;
    if (LibStr.isNotEmptyOrNull(value)) {
      for (final DateTimeFormatter formatter : formatters) {
        try {
          datetime = ZonedDateTime.parse(value, formatter);
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
    }
    if ((datetime == null) && (silentError)) {
      result.value = System.currentTimeMillis();
      result.valid = true;
    }
    if (datetime != null) {
      result.value = datetime.toInstant().toEpochMilli();
      result.valid = true;
    }
    return result;
  }

}
