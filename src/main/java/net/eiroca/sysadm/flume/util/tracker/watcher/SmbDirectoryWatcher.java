/**
 *
 * Copyright (C) 1999-2019 Enrico Croce - AGPL >= 3.0
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

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import com.google.common.collect.Lists;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import net.eiroca.ext.library.smb.ISMBFileVisitor;
import net.eiroca.ext.library.smb.LibSmb;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IWatcherResult;

public class SmbDirectoryWatcher extends Watcher implements Comparator<IWatcherResult> {

  transient private static final Logger logger = Logs.getLogger();

  // system time in milliseconds, stores the last modification time of the
  // parent directory seen by the last check, rounded to seconds
  // initial value is used in first check only when it will be replaced instantly
  // (system time is positive)
  private long lastSeenParentDirMTime = -1;
  // system time in milliseconds, time of the last check, rounded to seconds
  // initial value is used in first check only when it will be replaced instantly
  // (system time is positive)
  private long lastCheckedTime = -1;
  // cached content, files which matched the pattern within the parent directory
  transient private List<IWatcherResult> lastMatchedFiles = Lists.newArrayList();

  // directory monitored for changes
  transient private final SmbFile basePath;
  transient private final Pattern regex;

  public SmbDirectoryWatcher(final WatcherConfig config) throws MalformedURLException {
    super(config);
    if (LibStr.isNotEmptyOrNull(config.filter) && LibStr.isNotEmptyOrNull(config.path)) {
      basePath = LibSmb.build(config.path, config.principal);
      regex = Pattern.compile(config.filter);
    }
    else {
      basePath = null;
      regex = null;
    }
  }

  @Override
  public List<IWatcherResult> getMatchingFiles() {
    List<IWatcherResult> result = Lists.newArrayList();
    try {
      if ((basePath != null) && (basePath.exists())) {
        SmbDirectoryWatcher.logger.trace("getMatchingFiles()");
        final long now = TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
        final long currentParentDirMTime = basePath.lastModified();
        if (!config.cachePatternMatching || (lastSeenParentDirMTime < currentParentDirMTime) || !(currentParentDirMTime < lastCheckedTime) || ((now - lastCheckedTime) > config.maxCacheTime)) {
          lastMatchedFiles = sortByLastModifiedTime(getMatchingFilesNoCache());
          lastSeenParentDirMTime = currentParentDirMTime;
          lastCheckedTime = now;
        }
        result = lastMatchedFiles;
      }
    }
    catch (final SmbException e) {
      SmbDirectoryWatcher.logger.info("SmbException in DirectoryWatching: " + getName() + " -> " + basePath, e);
    }
    return result;
  }

  private List<IWatcherResult> getMatchingFilesNoCache() throws SmbException {
    SmbDirectoryWatcher.logger.trace("getMatchingFilesNoCache()");
    final List<IWatcherResult> result = Lists.newArrayList();
    LibSmb.walkFileTree(basePath, 0, new ISMBFileVisitor() {

      @Override
      public boolean visitFile(final SmbFile f) throws SmbException {
        SmbDirectoryWatcher.logger.trace("Looking {}", f.getCanonicalPath());
        boolean valid = regex.matcher(f.getCanonicalPath()).find();
        if (valid) {
          if (config.maxAge > 0) {
            final long now = System.currentTimeMillis();
            if (((now - f.lastModified())) > config.maxAge) {
              valid = false;
            }
          }
          if (valid) {
            result.add(new SmbFileWatcherResult(f));
          }
        }
        return true;
      }
    });
    return result;
  }

  /**
   * Utility function to sort matched files based on last modification time. Sorting itself use only
   * a snapshot of last modification times captured before the sorting to keep the number of stat
   * system calls to the required minimum.
   *
   * @param files list of files in any order
   * @return sorted list
   */
  transient final private HashMap<IWatcherResult, Long> lastModificationTimes = new HashMap<>();

  private List<IWatcherResult> sortByLastModifiedTime(final List<IWatcherResult> files) {
    lastModificationTimes.clear();
    for (final IWatcherResult f : files) {
      lastModificationTimes.put(f, f.getUpdateDate());
    }
    Collections.sort(files, this);
    return files;
  }

  @Override
  public int compare(final IWatcherResult o1, final IWatcherResult o2) {
    return lastModificationTimes.get(o1).compareTo(lastModificationTimes.get(o2));
  }

}
