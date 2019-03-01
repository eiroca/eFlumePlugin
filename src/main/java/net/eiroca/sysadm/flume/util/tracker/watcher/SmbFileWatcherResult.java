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

import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import net.eiroca.ext.library.smb.LibSmb;
import net.eiroca.sysadm.flume.api.ext.ITrackedSource;
import net.eiroca.sysadm.flume.util.tracker.source.TrackedSMB;

public class SmbFileWatcherResult extends WatcherResult {

  private transient SmbFile file;

  public SmbFileWatcherResult(final SmbFile f) throws SmbException {
    file = f;
    source = file.getURL().toString();
    size = file.length();
    updateDate = file.lastModified();
    id = LibSmb.getID(file);
  }

  @Override
  public ITrackedSource build(final WatcherConfig config, final long commitPos) {
    return new TrackedSMB(file, commitPos, config);
  }
}
