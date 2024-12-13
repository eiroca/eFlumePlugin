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
package net.eiroca.sysadm.flume.util.tracker.watcher;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.slf4j.Logger;
import com.google.common.collect.Lists;
import jcifs.smb.SmbAuthException;
import jcifs.smb.SmbFile;
import net.eiroca.ext.library.smb.LibSmb;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IWatcherResult;

public class DatedSmbFileWatcher extends Watcher {

  transient private static final Logger logger = Logs.getLogger();

  private final DateTimeFormatter dateFormat;

  public DatedSmbFileWatcher(final WatcherConfig config) {
    super(config);
    dateFormat = DateTimeFormatter.ofPattern(config.path);
  }

  @Override
  public List<IWatcherResult> getMatchingFiles() {
    final List<IWatcherResult> result = Lists.newArrayList();
    SmbFile f;
    try {
      final String path = dateFormat.format(LocalDate.now());
      f = LibSmb.build(path, config.principal);
      if (f != null) {
        DatedSmbFileWatcher.logger.debug("Checking " + f.getPath() + " exists:" + f.exists() + " isFile:" + f.isFile());
        if (f.exists() && f.isFile()) {
          result.add(new SmbFileWatcherResult(f));
        }
      }
    }
    catch (final SmbAuthException e) {
      DatedSmbFileWatcher.logger.warn(String.format("Error watching %s user: %s", config.name, config.principal), e);
    }
    catch (final Exception e) {
      DatedSmbFileWatcher.logger.warn("Error watching {}", config.name, e);
    }
    return result;
  }

}