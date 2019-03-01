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
package net.eiroca.sysadm.flume.core.util;

import com.google.common.collect.ImmutableMap;
import net.eiroca.ext.library.gson.JSonUtil;
import net.eiroca.library.parameter.Parameters;
import net.eiroca.sysadm.flume.api.IConfigurable;
import net.eiroca.sysadm.flume.api.INamedObject;

public class ConfigurableObject implements INamedObject, IConfigurable {

  transient protected final Parameters params = new Parameters();

  protected String name;

  public ConfigurableObject() {
    this(null);
  }

  public ConfigurableObject(final String name) {
    super();
    if (name == null) {
      this.name = getClass().getSimpleName();
    }
    else {
      this.name = name;
    }
  }

  @Override
  public void setName(final String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isConfigurable() {
    return params.count() > 0;
  }

  @Override
  public String toString() {
    return JSonUtil.toJSON(this);
  }

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.laodConfig(config, prefix);
  }

}
