/**
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.type.filter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.LibFile;
import net.eiroca.sysadm.flume.core.filters.FilterBlackOrWhite;

public class FilterKeywords extends FilterBlackOrWhite {

  final private transient ListParameter pFilterWhiteList = new ListParameter(params, "whitelist", null);
  final private transient ListParameter pFilterBlackList = new ListParameter(params, "blacklist", null);

  final private transient StringParameter pFilterWhiteListFile = new StringParameter(params, "whitelist-file", null);
  final private transient StringParameter pFilterBlackListFile = new StringParameter(params, "blacklist-file", null);

  public Set<String> whiteList;
  public Set<String> blackList;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    whiteList = new HashSet<>();
    blackList = new HashSet<>();
    FilterKeywords.addKeys(whiteList, pFilterWhiteList.get(), ignoreCase);
    FilterKeywords.addKeys(blackList, pFilterBlackList.get(), ignoreCase);
    FilterKeywords.addKeys(whiteList, pFilterWhiteListFile.get(), ignoreCase);
    FilterKeywords.addKeys(blackList, pFilterBlackListFile.get(), ignoreCase);
  }

  private static void addKeys(final Set<String> list, final String path, final boolean ignoreCase) {
    if (path == null) { return; }
    final List<String> keys = new ArrayList<>();
    LibFile.readStrings(path, keys);
    if (keys.size() > 0) {
      for (final String key : keys) {
        list.add(ignoreCase ? key.toLowerCase() : key);
      }
    }
  }

  private static void addKeys(final Set<String> list, final String[] keys, final boolean ignoreCase) {
    if (keys == null) { return; }
    for (final String key : keys) {
      list.add(ignoreCase ? key.toLowerCase() : key);
    }
  }

  @Override
  public boolean isInWhiteList(final String match) {
    final boolean result = (whiteList != null) ? whiteList.contains(match) : false;
    return result;
  }

  @Override
  public boolean isInBlackList(final String match) {
    final boolean result = (blackList != null) ? blackList.contains(match) : false;
    return result;
  }

}
