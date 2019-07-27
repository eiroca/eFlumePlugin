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
package net.eiroca.sysadm.flume.core.extractors;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.extractor.RegExExtractor;
import net.eiroca.sysadm.flume.type.extractor.SpacerExtractor;
import net.eiroca.sysadm.flume.type.extractor.SplitterExtractor;
import net.eiroca.sysadm.flume.type.extractor.WebLogExtractor;

public class Extractors {

  public static final Registry registry = new Registry();

  static {
    Extractors.registry.addEntry(RegExExtractor.class.getName());
    Extractors.registry.addEntry("regex", RegExExtractor.class.getName());
    Extractors.registry.addEntry("pattern", RegExExtractor.class.getName());
    Extractors.registry.addEntry("pattern", RegExExtractor.class.getName());
    Extractors.registry.addEntry("spacer", SpacerExtractor.class.getName());
    Extractors.registry.addEntry("space", SpacerExtractor.class.getName());
    Extractors.registry.addEntry("separator", SplitterExtractor.class.getName());
    Extractors.registry.addEntry("split", SplitterExtractor.class.getName());
    Extractors.registry.addEntry("splitter", SplitterExtractor.class.getName());
    Extractors.registry.addEntry("weblog", WebLogExtractor.class.getName());
  }

  public static IExtractor build(final String type, final ImmutableMap<String, String> config, final String prefix) {
    return (IExtractor)FlumeHelper.buildIConfigurable(Extractors.registry.className(type), config, prefix);
  }

}
