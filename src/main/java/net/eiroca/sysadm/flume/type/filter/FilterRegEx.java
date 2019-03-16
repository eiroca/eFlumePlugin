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
package net.eiroca.sysadm.flume.type.filter;

import java.util.regex.Pattern;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.RegExParameter;
import net.eiroca.library.system.LibRegEx;
import net.eiroca.sysadm.flume.core.util.FilterBlackOrWhite;

public class FilterRegEx extends FilterBlackOrWhite {

  final private transient RegExParameter pFilterWhiteList = new RegExParameter(params, "whitelist", null);
  final private transient RegExParameter pFilterBlackList = new RegExParameter(params, "blacklist", null);

  public Pattern filterWhiteList;
  public Pattern filterBlackList;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    filterWhiteList = pFilterWhiteList.get();
    filterBlackList = pFilterBlackList.get();
  }

  @Override
  public boolean isInWhiteList(final String match) {
    return (filterWhiteList != null) ? LibRegEx.find(filterWhiteList, match, filterMatchLimit) : false;
  }

  @Override
  public boolean isInBlackList(final String match) {
    return (filterBlackList != null) ? LibRegEx.find(filterBlackList, match, filterMatchLimit) : false;
  }

}
