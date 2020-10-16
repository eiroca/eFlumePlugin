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

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.filters.FilterBlackOrWhite;

public class FilterContains extends FilterBlackOrWhite {

  final private transient StringParameter pFilterWhiteList = new StringParameter(params, "whitelist", null);
  final private transient StringParameter pFilterBlackList = new StringParameter(params, "blacklist", null);

  public String filterWhiteList;
  public String filterBlackList;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    filterWhiteList = pFilterWhiteList.get();
    filterBlackList = pFilterBlackList.get();
    if (ignoreCase) {
      filterWhiteList = (filterWhiteList != null) ? filterWhiteList.toLowerCase() : null;
      filterBlackList = (filterBlackList != null) ? filterBlackList.toLowerCase() : null;
    }
  }

  @Override
  public boolean isInWhiteList(final String match) {
    return (filterWhiteList != null) ? match.contains(filterWhiteList) : false;
  }

  @Override
  public boolean isInBlackList(final String match) {
    return (filterBlackList != null) ? match.contains(filterBlackList) : false;
  }

}
