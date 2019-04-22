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
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.regex.ARegEx;
import net.eiroca.library.regex.RegularExpression;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.HeaderAction;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

public class HeaderRegEx extends HeaderAction {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pRegExSource = new StringParameter(params, "source", " %() ");
  final private transient StringParameter pRegExPattern = new StringParameter(params, "pattern", "\\s(.*)\\s");
  final private transient IntegerParameter pRegExLimit = new IntegerParameter(params, "size-limit", 16 * 1024);
  final private transient IntegerParameter pRegExEngine = new IntegerParameter(params, "regex-engine", 0);
  final private transient IntegerParameter pRegExMinSize = new IntegerParameter(params, "min-size", 512);
  final private transient IntegerParameter pRegExMaxTime = new IntegerParameter(params, "max-time", 100);

  public String source;
  public ARegEx rule;

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
    rule = RegularExpression.build(pRegExPattern.get(), pRegExEngine.get());
    rule.sizeLimit = pRegExLimit.get();
    rule.sizeMin = pRegExMinSize.get();
    rule.timeLimit = pRegExMaxTime.get();
  }

  @Override
  public String getValue(final Map<String, String> headers, final String body) {
    final String text = MacroExpander.expand(source, headers, body);
    HeaderRegEx.logger.trace("looking \"{}\" in \"{}\"", rule.pattern, text);
    final String value = rule.findFirst(text);
    if (value != null) {
      HeaderRegEx.logger.trace("{}->{}", name, value);
    }
    return value;
  }

}
