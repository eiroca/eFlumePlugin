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

import com.google.gson.Gson;
import net.eiroca.sysadm.flume.api.ext.IWatcherResult;

abstract public class WatcherResult implements IWatcherResult {

  protected String id;
  protected String source;
  protected long size;
  protected long updateDate;

  @Override
  public String getID() {
    return id;
  }

  @Override
  public String getSource() {
    return source;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public long getUpdateDate() {
    return updateDate;
  }

  @Override
  public String toString() {
    return new Gson().toJson(this).toString();
  }
}
