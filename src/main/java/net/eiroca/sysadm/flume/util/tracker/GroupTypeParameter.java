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
package net.eiroca.sysadm.flume.util.tracker;

import net.eiroca.library.config.Parameter;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.core.LibStr;

public class GroupTypeParameter extends Parameter<GroupType> {

  public GroupTypeParameter(final Parameters owner, final String paramName, final GroupType paramDef) {
    super(owner, paramName, paramDef);
  }

  public GroupTypeParameter(final Parameters owner, final String paramName) {
    super(owner, paramName);
  }

  @Override
  public GroupType convertString(final String strValue) {
    GroupType value;
    if (LibStr.isEmptyOrNull(strValue)) {
      value = getDefault();
    }
    else {
      try {
        value = GroupType.valueOf(strValue.toUpperCase());
      }
      catch (final IllegalArgumentException e) {
        value = getDefault();
      }
    }
    return value;
  }

  @Override
  public boolean isValid(final Object value) {
    return value instanceof GroupType;
  }

}
