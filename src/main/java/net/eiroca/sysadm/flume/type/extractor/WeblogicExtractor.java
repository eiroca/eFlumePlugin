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
package net.eiroca.sysadm.flume.type.extractor;

import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.CharParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.core.LibParser;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.extractors.Extractors;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

public class WeblogicExtractor extends ConfigurableObject implements IExtractor {

  static {
    Extractors.registry.addEntry("weblogic", WeblogicExtractor.class.getName());
  }

  transient private static final Logger logger = Logs.getLogger();

  final private transient ListParameter pFields = new ListParameter(params, "field-names", new String[] {
      "timestamp", "instance", "priority", "errorCode", "logger", "message"
  });

  final private transient CharParameter pSeparator = new CharParameter(params, "separator", ' ');
  final private transient CharParameter pBracketOpen = new CharParameter(params, "bracket-open", '[');
  final private transient CharParameter pBracketClose = new CharParameter(params, "bracket-close", ']');
  final private transient CharParameter pBracketSeparator = new CharParameter(params, "bracket-separator", ':');

  public String[] fields;
  public char separator;
  public char bracketOpen;
  public char bracketClose;
  public char bracketSeparator;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    WeblogicExtractor.logger.trace("config {}: {}", prefix, config);
    fields = pFields.get();
    separator = pSeparator.get();
    bracketOpen = pBracketOpen.get();
    bracketClose = pBracketClose.get();
    bracketSeparator = pBracketSeparator.get();
  }

  @Override
  public Tags getTags(final String value) {
    if (value == null) { return null; }
    WeblogicExtractor.logger.debug("Source: " + value);
    final Tags result = LibParser.splitTagAndMessage(value, separator, bracketOpen, bracketClose, bracketSeparator, fields);
    return result;
  }

}
