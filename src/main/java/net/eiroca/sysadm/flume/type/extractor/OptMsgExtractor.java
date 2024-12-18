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

import java.util.List;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.CharParameter;
import net.eiroca.library.core.LibParser;
import net.eiroca.library.system.Logs;

public class OptMsgExtractor extends SpacerExtractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient CharParameter pSeparator = new CharParameter(params, "separator", ' ');
  final private transient CharParameter pSpecial1 = new CharParameter(params, "special-1", ':');
  final private transient CharParameter pSpecial2 = new CharParameter(params, "special-2", '-');

  public char separator;
  public char special1;
  public char special2;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    OptMsgExtractor.logger.trace("config {}: {}", prefix, config);
    special1 = pSpecial1.get();
    special2 = pSpecial2.get();
    separator = pSeparator.get();
  }

  @Override
  public List<String> getValues(final String value) {
    if ((fields == null) || (value == null)) { return null; }
    OptMsgExtractor.logger.debug("Source: " + value);
    final List<String> result = LibParser.splitOptAndMessage(value, separator, special1, special2);
    return result;
  }

}
