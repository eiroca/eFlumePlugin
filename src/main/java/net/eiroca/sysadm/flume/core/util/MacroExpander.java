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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.slf4j.Logger;
import net.eiroca.library.core.LibFormat;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;

public class MacroExpander {

  // Expand following macros
  // %[ip] -> IP
  // %[fqdn] -> fqdn
  // %[localhost] -> localhost
  // %[system.property] -> system.property value
  // %["valid SimpleDateFormat"] -> formatted System.millies
  //
  // %\w -> unix date formatter
  //
  // %{header_name} -> header_value
  //
  // %() -> body
  //
  // %(name) -> fields.get(name)
  //

  private static final String EMPTY = "";

  transient private static final Logger logger = Logs.getLogger();

  // ---
  private static final String HEADER_TIMESTAMP = "timestamp";

  private static final String GLOBAL_FQDN = "fqdn";
  private static final String GLOBAL_IP = "ip";
  private static final String GLOBAL_LOCALHOST = "localhost";

  private static final HashMap<String, String> GLOBALS = new HashMap<>();
  static {
    try {
      final InetAddress addr = InetAddress.getLocalHost();
      MacroExpander.GLOBALS.put(MacroExpander.GLOBAL_LOCALHOST, addr.getHostName());
      MacroExpander.GLOBALS.put(MacroExpander.GLOBAL_IP, addr.getHostAddress());
      MacroExpander.GLOBALS.put(MacroExpander.GLOBAL_FQDN, addr.getCanonicalHostName());
    }
    catch (final UnknownHostException e) {
      MacroExpander.logger.error("Unable to retrieve localhost information", e);
    }
  }

  /**
   * Not intended as a public API
   */
  private static String replaceShorthand(final char c, final Map<String, String> headers, final TimeZone timeZone, final boolean needRounding, final int unit, final int roundDown, final boolean useLocalTimestamp, long ts) {
    if (!useLocalTimestamp) {
      String timestampHeader = null;
      timestampHeader = (headers != null) ? headers.get(MacroExpander.HEADER_TIMESTAMP) : null;
      if (timestampHeader == null) {
        MacroExpander.logger.info("MacroExpand - Missing timestamp");
        timestampHeader = String.valueOf(ts);
      }
      try {
        ts = Long.valueOf(timestampHeader);
      }
      catch (final NumberFormatException e) {
        throw new RuntimeException("MacroExpander wasn't able to parse timestamp header in the event to resolve time based bucketing. Please check that you're correctly populating timestamp header (for example using TimestampInterceptor source interceptor).", e);
      }
    }
    if (needRounding) {
      ts = FlumeHelper.roundDown(roundDown, unit, ts, timeZone);
    }
    // It's a date
    String formatString = MacroExpander.EMPTY;
    switch (c) {
      case '%':
        return "%";
      case '$':
        return "$";
      case '0':
        formatString = "SSS";
        break;
      case 'a':
        formatString = "EEE";
        break;
      case 'A':
        formatString = "EEEE";
        break;
      case 'b':
        formatString = "MMM";
        break;
      case 'B':
        formatString = "MMMM";
        break;
      case 'c':
        formatString = "EEE MMM d HH:mm:ss yyyy";
        break;
      case 'd':
        formatString = "dd";
        break;
      case 'e':
        formatString = "d";
        break;
      case 'D':
        formatString = "MM/dd/yy";
        break;
      case 'H':
        formatString = "HH";
        break;
      case 'I':
        formatString = "hh";
        break;
      case 'j':
        formatString = "DDD";
        break;
      case 'k':
        formatString = "H";
        break;
      case 'l':
        formatString = "h";
        break;
      case 'm':
        formatString = "MM";
        break;
      case 'M':
        formatString = "mm";
        break;
      case 'n':
        formatString = "M";
        break;
      case 'p':
        formatString = "a";
        break;
      case 's':
        return MacroExpander.EMPTY + (ts / 1000);
      case 'S':
        formatString = "ss";
        break;
      case 't':
        // This is different from unix date (which would insert a tab character
        // here)
        return String.valueOf(ts);
      case 'y':
        formatString = "yy";
        break;
      case 'Y':
        formatString = "yyyy";
        break;
      case 'z':
        formatString = "z";
        break;
      case 'Z':
        formatString = "Z";
        break;
      default:
        MacroExpander.logger.warn("Unrecognized escape in event format string: %" + c);
        return MacroExpander.EMPTY;
    }
    final SimpleDateFormat format = LibFormat.getSimpleDateFormat(formatString);
    if (timeZone != null) {
      format.setTimeZone(timeZone);
    }
    else {
      format.setTimeZone(TimeZone.getDefault());
    }
    final Date date = new Date(ts);
    return format.format(date);
  }

  /**
   * Not intended as a public API
   */
  private static String replaceStatic(final String key) {
    String replacementString = null;
    try {
      final boolean isDatetime = (key.length() > 2) && key.startsWith("\"") && key.endsWith("\"");
      if (isDatetime) {
        MacroExpander.logger.error("Processing datetime: " + key.substring(1, key.length() - 1));
        final SimpleDateFormat format = LibFormat.getSimpleDateFormat(key.substring(1, key.length() - 1));
        replacementString = format.format(new Date());
      }
      else {
        replacementString = MacroExpander.GLOBALS.get(key.toLowerCase());
        if (replacementString == null) {
          MacroExpander.logger.error("Processing property: " + key);
          replacementString = System.getProperty(key);
        }
      }
    }
    catch (final Exception e) {
      replacementString = null;
    }
    if (replacementString == null) { throw new RuntimeException("MacroExpander wasn't able to parse the static escape sequence '" + key + "'"); }
    return replacementString;
  }

  // -----
  /**
   * Replace all macro. Any unrecognised / not found tags will be replaced with the empty string.
   *
   * @param needRounding - Should the timestamp be rounded down?
   * @param unit - if needRounding is true, what unit to round down to. This must be one of the
   *          units specified by {@link java.util.Calendar} - HOUR, MINUTE or SECOND. Defaults to
   *          second, if none of these are present. Ignored if needRounding is false.
   * @param roundDown - if needRounding is true, The time should be rounded to the largest multiple
   *          of this value, smaller than the time supplied, defaults to 1, if <= 0(rounds off to
   *          the second/minute/hour immediately lower than the timestamp supplied. Ignored if
   *          needRounding is false.
   * @return Escaped string.
   */
  public static String expand(final String macro, final Map<String, String> headers, final String body, final Map<String, Object> fields, final TimeZone timeZone, final boolean needRounding, final int unit, final int roundDown, final boolean useLocalTimeStamp) {
    if (macro == null) { return null; }
    if (macro.equals("%()")) { return (body != null) ? body : MacroExpander.EMPTY; }
    final long ts = System.currentTimeMillis();
    int i = 0, p = 0, s = 0;
    char ch, nc, c;
    String name;
    Object replacement;
    final int size = macro.length();
    final StringBuilder sb = new StringBuilder(size + 64);
    while (i < size) {
      ch = macro.charAt(i);
      if (ch == '%') {
        if (s < i) {
          sb.append(macro, s, i);
        }
        i++;
        nc = macro.charAt(i);
        p = i + 1;
        switch (nc) {
          case '[':
            i++;
            while (i < size) {
              c = macro.charAt(i);
              i++;
              if (c == ']') {
                break;
              }
            }
            name = macro.substring(p, i - 1);
            sb.append(MacroExpander.replaceStatic(name));
            break;
          case '(':
            i++;
            while (i < size) {
              c = macro.charAt(i);
              i++;
              if (c == ')') {
                break;
              }
            }
            name = macro.substring(p, i - 1);
            if (LibStr.isEmptyOrNull(name)) {
              if (body != null) {
                sb.append(body);
              }
            }
            else if (fields != null) {
              replacement = fields.get(name);
              if (replacement != null) {
                sb.append(replacement);
              }
            }
            break;
          case '{':
            i++;
            while (i < size) {
              c = macro.charAt(i);
              i++;
              if (c == '}') {
                break;
              }
            }
            name = macro.substring(p, i - 1);
            replacement = headers.get(name);
            if (replacement != null) {
              sb.append(replacement);
            }
            break;
          default:
            i++;
            sb.append(MacroExpander.replaceShorthand(nc, headers, timeZone, needRounding, unit, roundDown, useLocalTimeStamp, ts));
            break;
        }
        s = i;
      }
      else {
        i++;
      }
    }
    if (s == 0) { return macro; }
    if (s < i) {
      sb.append(macro, s, i);
    }
    return sb.toString();
  }

  public static String expand(final String macro, final Map<String, String> headers) {
    return MacroExpander.expand(macro, headers, null, null, null, false, 0, 0, false);
  }

  public static String expand(final String macro, final Map<String, String> headers, final String body) {
    return MacroExpander.expand(macro, headers, body, null, null, false, 0, 0, false);
  }

  public static String expand(final String macro, final Map<String, String> headers, final String body, final Map<String, Object> fields) {
    return MacroExpander.expand(macro, headers, body, fields, null, false, 0, 0, false);
  }

}
