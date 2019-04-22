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

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import com.google.common.collect.Lists;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.ext.IWatcherResult;
import net.eiroca.sysadm.flume.plugin.TrackerSource;

/**
 * Identifies and caches the files matched by single file pattern for {@code TrackerSource} source.
 * <p>
 * </p>
 * Since file patterns only apply to the fileNames and not the parent dictionaries, this
 * implementation checks the parent directory for modification (additional or removed files update
 * modification time of parent dir) If no modification happened to the parent dir that means the
 * underlying files could only be written to but no need to rerun the pattern matching on fileNames.
 * <p>
 * </p>
 * This implementation provides lazy caching or no caching. Instances of this class keep the result
 * file list from the last successful execution of {@linkplain #getMatchingFiles()} function
 * invocation, and may serve the content without hitting the FileSystem for performance
 * optimization.
 * <p>
 * </p>
 * <b>IMPORTANT:</b> It is assumed that the hosting system provides at least second granularity for
 * both {@code System.currentTimeMillis()} and {@code File.lastModified()}. Also that system clock
 * is used for file system timestamps. If it is not the case then configure it as uncached. Class is
 * solely for package only usage. Member functions are not thread safe.
 *
 * @see TrackerSource
 * @see TrackerEventReader
 */
public class DirectoryWatcher extends Watcher implements DirectoryStream.Filter<Path>, Comparator<IWatcherResult> {

  transient private static final Logger logger = Logs.getLogger();
  transient private static final FileSystem FS = FileSystems.getDefault();

  // directory monitored for changes
  private final File parentDir;
  // cached instance for filtering files based on filePattern
  transient private final DirectoryStream.Filter<Path> fileFilter;

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
  final PathMatcher matcher;

  /**
   * Package accessible constructor. From configuration context it represents a single
   * <code>filegroup</code> and encapsulates the corresponding <code>filePattern</code>.
   * <code>filePattern</code> consists of two parts: first part has to be a valid path to an
   * existing parent directory, second part has to be a valid regex {@link java.util.regex.Pattern}
   * that match any non-hidden file names within parent directory . A valid example for filePattern
   * is <code>/dir0/dir1/.*</code> given <code>/dir0/dir1</code> is an existing directory structure
   * readable by the running user.
   * <p>
   * </p>
   * An instance of this class is created for each fileGroup
   *
   * @param sourceGroup arbitrary name of the group given by the config
   * @param sourcePattern parent directory plus regex pattern. No wildcards are allowed in directory
   *          name
   * @param cachePatternMatching default true, recommended in every setup especially with huge
   *          parent directories. Don't set when local system clock is not used for stamping mtime
   *          (eg: remote filesystems)
   */
  public DirectoryWatcher(final WatcherConfig config) {
    super(config);
    parentDir = new File(config.path);
    matcher = DirectoryWatcher.FS.getPathMatcher("regex:" + config.filter);
    fileFilter = this;
  }

  /**
   * Lists those files within the parentDir that match regex pattern passed in during object
   * instantiation. Designed for frequent periodic invocation
   * {@link org.apache.flume.source.PollableSourceRunner}.
   * <p>
   * </p>
   * Based on the modification of the parentDir this function may trigger cache recalculation by
   * calling {@linkplain #getMatchingFilesNoCache()} or return the value stored in
   * {@linkplain #lastMatchedFiles}. Parentdir is allowed to be a symbolic link.
   * <p>
   * </p>
   * Files returned by this call are weakly consistent (see {@link DirectoryStream}). It does not
   * freeze the directory while iterating, so it may (or may not) reflect updates to the directory
   * that occur during the call, In which case next call will return those files (as mtime is
   * increasing it won't hit cache but trigger recalculation). It is guaranteed that invocation
   * reflects every change which was observable at the time of invocation.
   * <p>
   * </p>
   * Matching file list recalculation is triggered when caching was turned off or if mtime is
   * greater than the previously seen mtime (including the case of cache hasn't been calculated
   * before). Additionally if a constantly updated directory was configured as parentDir then
   * multiple changes to the parentDir may happen within the same second so in such case (assuming
   * at least second granularity of reported mtime) it is impossible to tell whether a change of the
   * dir happened before the check or after (unless the check happened after that second). Having
   * said that implementation also stores system time of the previous invocation and previous
   * invocation has to happen strictly after the current mtime to avoid further cache refresh
   * (because then it is guaranteed that previous invocation resulted in valid cache content). If
   * system clock hasn't passed the second of the current mtime then logic expects more changes as
   * well (since it cannot be sure that there won't be any further changes still in that second and
   * it would like to avoid data loss in first place) hence it recalculates matching files. If
   * system clock finally passed actual mtime then a subsequent invocation guarantees that it picked
   * up every change from the passed second so any further invocations can be served from cache
   * associated with that second (given mtime is not updated again).
   *
   * @return List of files matching the pattern sorted by last modification time. No recursion. No
   *         directories. If nothing matches then returns an empty list. If I/O issue occurred then
   *         returns the list collected to the point when exception was thrown.
   *
   * @see #getMatchingFilesNoCache()
   */
  @Override
  public List<IWatcherResult> getMatchingFiles() {
    DirectoryWatcher.logger.trace("Checking files: {}", parentDir.exists());
    if (!parentDir.exists()) { return Lists.newArrayList(); }
    final long now = TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()));
    final long currentParentDirMTime = parentDir.lastModified();
    // calculate matched files if
    // - we don't want to use cache (recalculate every time) OR
    // - directory was clearly updated after the last check OR
    // - last mtime change wasn't already checked for sure
    // (system clock hasn't passed that second yet)
    if (!config.cachePatternMatching || (lastSeenParentDirMTime < currentParentDirMTime) || !(currentParentDirMTime < lastCheckedTime)) {
      lastMatchedFiles = sortByLastModifiedTime(getMatchingFilesNoCache());
      lastSeenParentDirMTime = currentParentDirMTime;
      lastCheckedTime = now;
    }
    return lastMatchedFiles;
  }

  /**
   * Provides the actual files within the parentDir which files are matching the regex pattern. Each
   * invocation uses {@link DirectoryStream} to identify matching files.
   *
   * Files returned by this call are weakly consistent (see {@link DirectoryStream}). It does not
   * freeze the directory while iterating, so it may (or may not) reflect updates to the directory
   * that occur during the call. In which case next call will return those files.
   *
   * @return List of files matching the pattern unsorted. No recursion. No directories. If nothing
   *         matches then returns an empty list. If I/O issue occurred then returns the list
   *         collected to the point when exception was thrown.
   *
   * @see DirectoryStream
   * @see DirectoryStream.Filter
   */
  private List<IWatcherResult> getMatchingFilesNoCache() {
    DirectoryWatcher.logger.trace("Checking files (nocache): {}", parentDir.exists());
    final List<IWatcherResult> result = Lists.newArrayList();
    if (parentDir.exists()) {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(parentDir.toPath(), fileFilter)) {
        for (final Path entry : stream) {
          File f = entry.toFile();
          DirectoryWatcher.logger.trace("Checking files {}", f.getAbsolutePath());
          if (config.maxAge > 0) {
            final long now = System.currentTimeMillis();
            if (((now - f.lastModified())) > config.maxAge) {
              f = null;
            }
          }
          if (f != null) {
            DirectoryWatcher.logger.trace("Adding files {}", f.getAbsolutePath());
            result.add(new FileWatcherResult(f));
          }
        }
      }
      catch (final IOException e) {
        DirectoryWatcher.logger.error("I/O exception occurred while listing '{}'. Files already matched will be returned.", parentDir.toPath(), e);
      }
    }
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
  public boolean accept(final Path entry) throws IOException {
    return matcher.matches(entry.getFileName()) && !Files.isDirectory(entry);
  }

  @Override
  public int compare(final IWatcherResult o1, final IWatcherResult o2) {
    return lastModificationTimes.get(o1).compareTo(lastModificationTimes.get(o2));
  }

}
