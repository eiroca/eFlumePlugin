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
package net.eiroca.sysadm.flume.core.extractors;

import java.util.List;
import net.eiroca.library.data.Tags;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

abstract public class Extractor extends ConfigurableObject implements IExtractor {

  abstract public List<String> getNames();

  abstract public List<String> getValues(final String value);

  public List<String> getAltNames() {
    return null;
  }

  public boolean hasNames() {
    return getNames() != null;
  }

  public boolean hasAltNames() {
    return getAltNames() != null;
  }

  @Override
  public Tags getTags(String value) {
    if (value == null) return null;
    final List<String> names = getNames();
    final List<String> altNames = getAltNames();
    final List<String> values = getValues(value);
    final Tags tags = new Tags();
    tags.setTagFormat("%s: %s");
    tags.setDefaultTagValue("");
    if ((names != null) && (names.size() == values.size())) {
      tags.addValues(names, values);
    }
    else if ((altNames != null) && (altNames.size() == values.size())) {
      tags.addValues(altNames, values);
    }
    else {
      tags.addValues(values);
    }
    return tags;
  }

}
