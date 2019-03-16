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
package net.eiroca.sysadm.flume.util.tracker.watcher;

import com.google.common.base.Preconditions;
import net.eiroca.ext.library.gson.GsonUtil;
import net.eiroca.sysadm.flume.api.ext.IWatcher;

abstract public class Watcher implements IWatcher {

  protected WatcherConfig config;

  public Watcher(final WatcherConfig config) {
    Preconditions.checkNotNull(config, "Invalid configuration");
    Preconditions.checkNotNull(config.name, "Invalid configuration");
    this.config = config;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) { return true; }
    if ((o == null) || (getClass() != o.getClass())) { return false; }
    final Watcher that = (Watcher)o;
    return config.name.equals(that.config.name);
  }

  @Override
  public int hashCode() {
    return config.name.hashCode();
  }

  @Override
  public String getName() {
    return config.name;
  }

  @Override
  public String toString() {
    return GsonUtil.toJSON(this);
  }

}
