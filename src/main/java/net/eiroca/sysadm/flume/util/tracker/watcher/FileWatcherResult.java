/**
 *
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
package net.eiroca.sysadm.flume.util.tracker.watcher;

import java.io.File;
import net.eiroca.library.system.LibFile;
import net.eiroca.sysadm.flume.api.ext.ITrackedSource;
import net.eiroca.sysadm.flume.util.tracker.source.TrackedFile;

public class FileWatcherResult extends WatcherResult {

  private transient File file;

  public FileWatcherResult(final File f) {
    file = f;
    id = LibFile.getFileKey(file);
    source = file.getAbsolutePath();
    size = file.length();
    updateDate = file.lastModified();
  }

  @Override
  public ITrackedSource build(final WatcherConfig config, final long commitPos) {
    return new TrackedFile(file, commitPos, config);
  }

}
