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
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.CharParameter;
import net.eiroca.library.core.LibParser;
import net.eiroca.sysadm.flume.core.extractors.Extractors;

public class SplitterExtractor extends SpacerExtractor {

  static {
    Extractors.registry.addEntry("separator", SplitterExtractor.class.getName());
    Extractors.registry.addEntry("split", SplitterExtractor.class.getName());
    Extractors.registry.addEntry("splitter", SplitterExtractor.class.getName());
  }

  final private transient CharParameter pSeparator = new CharParameter(params, "separator", ',');

  public char separator;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    separator = pSeparator.get();
  }

  @Override
  public List<String> getValues(final String value) {
    if ((fields == null) || (value == null)) { return null; }
    final List<String> result = LibParser.splitWithSep(value, separator, fields.length);
    return result;
  }

}
