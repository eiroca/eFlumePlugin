/**
 *
 * Copyright (C) 2001-2019 eIrOcA (eNrIcO Croce & sImOnA Burzio) - AGPL >= 3.0
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.RegExParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.HeaderAction;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

public class HeaderRegEx extends HeaderAction {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pRegExSource = new StringParameter(params, "source", " %() ");
  final private transient RegExParameter pRegExPattern = new RegExParameter(params, "pattern", "\\s(.*)\\s");
  final private transient IntegerParameter pRegExLimit = new IntegerParameter(params, "size-limit", 16 * 1024);
  final private transient IntegerParameter pRegExMinSize = new IntegerParameter(params, "min-size", 512);
  final private transient IntegerParameter pRegExMaxTime = new IntegerParameter(params, "max-time", 100);

  public String source;
  public Pattern rule;
  public int limit = -1;
  public int minSize = 1024;
  public int maxTime = 100;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    final String value = config.get(prefix);
    final String regexPrefix = LibStr.concatenate(prefix, ".");
    HeaderRegEx.logger.trace("prefix: {} name: {}", prefix, name);
    HeaderRegEx.logger.trace("regexprefix: {}", regexPrefix);
    params.loadConfig(config, regexPrefix);
    if (value == null) {
      force = true;
    }
    else {
      force = new Boolean(String.valueOf(value));
    }
    source = pRegExSource.get();
    rule = pRegExPattern.get();
    limit = pRegExLimit.get();
    minSize = pRegExMinSize.get();
    maxTime = pRegExMaxTime.get();
  }

  @Override
  public String getValue(final Map<String, String> headers, final String body) {
    final String value = runRegEx(headers, body);
    if (value != null) {
      HeaderRegEx.logger.trace("{}->{}", name, value);
    }
    return value;
  }

  protected String runRegEx(final Map<String, String> headers, final String body) {
    final long now = System.currentTimeMillis();
    String value = null;
    boolean crash = false;
    final String match = LibStr.limit(MacroExpander.expand(source, headers, body), limit);
    HeaderRegEx.logger.trace("looking \"{}\" in \"{}\"", rule.pattern(), match);
    boolean success = false;
    Matcher matcher = null;
    try {
      matcher = rule.matcher(match);
      success = matcher.find();
    }
    catch (final StackOverflowError err) {
      crash = true;
      HeaderRegEx.logger.info("Pattern too complex: {}", rule.pattern());
    }
    if (success) {
      value = (matcher != null) ? matcher.group(1) : null;
    }
    final long elapsed = (System.currentTimeMillis() - now);
    if ((elapsed >= maxTime) || crash) {
      final int theMinSize = (limit > 0) ? Math.min(minSize, limit) : minSize;
      int newLimit;
      if (limit < 1) {
        newLimit = match.length() / 2;
      }
      else {
        newLimit = limit / 2;
      }
      newLimit = Math.max(newLimit, theMinSize);
      if (limit != newLimit) {
        HeaderRegEx.logger.info(String.format("REGEX %6d ms LIMITED %6d->%6d: %s", elapsed, limit, newLimit, rule.pattern()));
        limit = newLimit;
      }
      else {
        HeaderRegEx.logger.warn(String.format("SLOW REGEX %6d ms: %s", elapsed, rule.pattern()));
      }
    }
    return value;
  }

}
