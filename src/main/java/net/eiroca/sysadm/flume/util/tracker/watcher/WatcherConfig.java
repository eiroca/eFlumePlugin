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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import jcifs.smb.NtlmPasswordAuthentication;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.parameter.BooleanParameter;
import net.eiroca.library.parameter.ByteParameter;
import net.eiroca.library.parameter.IntegerParameter;
import net.eiroca.library.parameter.Parameters;
import net.eiroca.library.parameter.RegExParameter;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.util.tracker.GroupType;
import net.eiroca.sysadm.flume.util.tracker.GroupTypeParameter;
import net.eiroca.sysadm.flume.util.tracker.TrackerManagerConfig;

public class WatcherConfig {

  transient private static final Logger logger = Logs.getLogger();

  private static final String HEADER_PREFIX = "header.";

  private static final String SMB_PREFIX = "smb:";
  private static final String FILE_PREFIX = "file:";
  private static final String DIR_PREFIX = "dir:";
  private static final String SMBREGEX_PREFIX = "regex:";
  private static final String REGEX_RULE1 = "..";
  private static final String REGEX_RULE2 = "*";

  final transient private Parameters params = new Parameters();

  final transient private GroupTypeParameter pGroupType = new GroupTypeParameter(params, "type", GroupType.UNKNOWN);
  final transient private StringParameter pPath = new StringParameter(params, "path", null);
  final transient private StringParameter pPrincipal = new StringParameter(params, "principal", null);
  final transient private StringParameter pFilter = new StringParameter(params, "filter", null);

  /**
   * Whether to cache the list of files matching the specified file patterns till parent directory
   * is modified.
   */
  final transient private BooleanParameter pCchePatternMatching = new BooleanParameter(params, "cache-pattern-matching", true);
  /** Max Age (in seconds) of matched file (now- last_modified) < maxAge. MaxAge = -1 -> no check */
  final transient private IntegerParameter pLocalMaxAge = new IntegerParameter(params, "max-age", -2);
  final transient private IntegerParameter pMaxCacheTime = new IntegerParameter(params, "max-cache-time", 60 * 1000);

  /** Share MODE */
  final transient private IntegerParameter pShareMode = new IntegerParameter(params, "share-mode", -1);

  final transient private StringParameter pEncoding = new StringParameter(params, "encoding", "utf-8");
  final transient private ByteParameter pDelimiter = new ByteParameter(params, "delimiter", (byte)'\n');
  final transient private ByteParameter pTrimmed = new ByteParameter(params, "trimmed", (byte)'\r');
  final transient private RegExParameter pMatchPattern = new RegExParameter(params, "pattern", null);
  final transient private IntegerParameter pSizeLimit = new IntegerParameter(params, "size-limit", 64);
  final transient private BooleanParameter pNegate = new BooleanParameter(params, "negate", false);
  final transient private BooleanParameter pNewEvent = new BooleanParameter(params, "new-event", true);
  final transient private StringParameter pSeparator = new StringParameter(params, "separator", "\r\n", true);
  final transient private IntegerParameter pBufferSize = new IntegerParameter(params, "buffer-size", 32 * 1024);
  final transient private IntegerParameter pKeepBlocks = new IntegerParameter(params, "keep-blocks", 20);
  final transient private IntegerParameter pMaxInvalidBlocks = new IntegerParameter(params, "max-invalid-blocks", 3);
  final transient private ByteParameter pInvalidChar = new ByteParameter(params, "invalid-char", (byte)0);

  public String name;
  public GroupType type = GroupType.UNKNOWN;
  public String path;
  public String filter;
  public transient NtlmPasswordAuthentication principal;

  public boolean cachePatternMatching;
  public int maxCacheTime;
  public long maxAge;
  public int shareMode;

  public String offsetHeaderName;
  public String sourceHeaderName;

  public String encoding;
  public byte delimiter;
  public byte trimmed;
  public Pattern matcher;
  public int sizeLimit;
  public boolean negate;
  public boolean newEvent;
  public byte[] separator;
  public int bufferSize;
  public int keepBlocks;
  public int maxInvalidBlocks;
  public byte invalidChar;

  public Map<String, String> headers;

  public WatcherConfig(final String name, final TrackerManagerConfig parent, final ImmutableMap<String, String> config, final String prefix) {
    this.name = name;
    configure(parent, config, prefix);
  }

  public void configure(final TrackerManagerConfig parent, final ImmutableMap<String, String> config, final String basePrefix) {
    String prefix = basePrefix;
    if (prefix != null) {
      prefix = prefix + ".";
    }
    WatcherConfig.logger.trace("TrackedFileConfig prefix: {} config: {}", prefix, config);
    params.laodConfig(config, prefix);
    //
    cachePatternMatching = pCchePatternMatching.get();
    int localMaxAge = pLocalMaxAge.get();
    if (localMaxAge == -2) {
      localMaxAge = parent.maxAge;
    }
    maxAge = (localMaxAge >= 0) ? localMaxAge * 1000 : -1;
    maxCacheTime = pMaxCacheTime.get();
    //
    shareMode = pShareMode.get();
    //
    encoding = pEncoding.get();
    delimiter = pDelimiter.get();
    trimmed = pTrimmed.get();
    matcher = pMatchPattern.get();
    sizeLimit = pSizeLimit.get();
    negate = pNegate.get();
    newEvent = pNewEvent.get();
    separator = pSeparator.get().getBytes();
    bufferSize = pBufferSize.get();
    keepBlocks = pKeepBlocks.get();
    maxInvalidBlocks = pMaxInvalidBlocks.get();
    invalidChar = pInvalidChar.get();
    //
    path = pPath.get();
    type = pGroupType.get();
    filter = pFilter.get();
    final String raw;
    if (basePrefix != null) {
      raw = config.get(basePrefix);
      if (path == null) {
        path = raw;
      }
    }
    Preconditions.checkNotNull(path, "Watchers configuration invalid (missing path, prefix: " + prefix + ")");
    if (type == GroupType.UNKNOWN) {
      type = autoDetectType();
    }
    if (LibStr.isEmptyOrNull(filter)) {
      switch (type) {
        case SMBREGEX:
          autodetect();
          break;
        case DIRECTORY:
          autodetect();
          break;
        default:
          break;
      }
    }
    readHeader(config, LibStr.concatenate(prefix, WatcherConfig.HEADER_PREFIX));
    //
    final String principal = pPrincipal.get();
    if (principal != null) {
      this.principal = parent.principalConfigs.get(principal);
    }
    if (shareMode < 0) {
      shareMode = parent.shareMode;
    }
    if (sourceHeaderName == null) {
      sourceHeaderName = parent.sourceHeaderName;
    }
    if (offsetHeaderName == null) {
      offsetHeaderName = parent.offsetHeaderName;
    }
  }

  @Override
  public String toString() {
    return new Gson().toJson(this).toString();
  }

  private void autodetect() {
    WatcherConfig.logger.trace("Autodetecting filter", this);
    String parentDir = path;
    boolean ok = false;
    int pos = parentDir.lastIndexOf('/');
    if (pos < 0) {
      pos = parentDir.lastIndexOf('\\');
    }
    if (pos > 0) {
      parentDir = parentDir.substring(0, pos + 1);
      ok = true;
    }
    if (ok) {
      filter = path.substring(pos + 1, path.length());
      if (LibStr.isEmptyOrNull(filter)) {
        filter = ".$";
      }
      else {
        filter = filter + "$";
      }
      path = parentDir;
    }
    WatcherConfig.logger.debug("Autodetect new config: {}", this);
  }

  public GroupType autoDetectType() {
    GroupType type;
    final boolean isRegEx = path.contains(WatcherConfig.REGEX_RULE1) || path.contains(WatcherConfig.REGEX_RULE2);
    if (path.startsWith(WatcherConfig.FILE_PREFIX)) {
      type = GroupType.FILE;
      path = path.substring(WatcherConfig.FILE_PREFIX.length());
    }
    else if (path.startsWith(WatcherConfig.DIR_PREFIX)) {
      type = GroupType.DIRECTORY;
      path = path.substring(WatcherConfig.DIR_PREFIX.length());
    }
    else if (path.startsWith(WatcherConfig.SMB_PREFIX)) {
      type = isRegEx ? GroupType.SMBREGEX : GroupType.SMBFILE;
    }
    else if (path.startsWith(WatcherConfig.SMBREGEX_PREFIX)) {
      type = GroupType.SMBREGEX;
      path = WatcherConfig.SMB_PREFIX + path.substring(WatcherConfig.SMBREGEX_PREFIX.length());
    }
    else if (isRegEx) {
      type = GroupType.DIRECTORY;
    }
    else {
      type = GroupType.FILE;
    }
    return type;
  }

  private void readHeader(final ImmutableMap<String, String> context, final String prefix) {
    if (headers == null) {
      headers = new HashMap<>();
    }
    else {
      headers.clear();
    }
    for (final Entry<String, String> e : context.entrySet()) {
      final String key = e.getKey();
      if (key.startsWith(prefix)) {
        final String header = key.substring(prefix.length());
        headers.put(header, e.getValue());
      }
    }
    if (headers.isEmpty()) {
      headers = null;
    }
  }

}
