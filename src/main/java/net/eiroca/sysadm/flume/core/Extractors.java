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
package net.eiroca.sysadm.flume.core;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.util.Flume;
import net.eiroca.sysadm.flume.type.extractor.RegExExtractor;

public class Extractors {

  public static final Registry registry = new Registry();

  static {
    Extractors.registry.addEntry(RegExExtractor.class.getName());
    Extractors.registry.addEntry("regex", RegExExtractor.class.getName());
    Extractors.registry.addEntry("pattern", RegExExtractor.class.getName());
    Extractors.registry.addEntry("pattern", RegExExtractor.class.getName());
  }

  public static IExtractor build(final String type, final ImmutableMap<String, String> config, final String prefix) {
    return (IExtractor)Flume.buildIConfigurable(Extractors.registry.className(type), config, prefix);
  }

}
