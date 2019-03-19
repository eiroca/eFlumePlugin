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
package net.eiroca.sysadm.flume.util.tracker;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import jcifs.smb.NtlmPasswordAuthenticator;
import net.eiroca.ext.library.gson.GsonUtil;
import net.eiroca.ext.library.smb.LibSmb;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.PathParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

public class TrackerManagerConfig {

  transient private static final Logger logger = Logs.getLogger();

  final transient private Parameters params = new Parameters();

  /** Interval time (ms) to write the last position of each file on the position file. */
  final transient private IntegerParameter pWritePosInternval = new IntegerParameter(params, "update-registry-interval", 10000);
  final transient private PathParameter pPositionFilePath = new PathParameter(params, "positionFile", "~/.flume/tracking_position.json");

  /** Time (ms) for idle files (flush incomplete events, close file, delete registry entry) */
  final transient private IntegerParameter pInactiveFlush = new IntegerParameter(params, "inactive-flush", 30 * 1000);
  final transient private IntegerParameter pInactiveClose = new IntegerParameter(params, "inactive-close", 5 * 60 * 1000);
  final transient private IntegerParameter pInactiveDelete = new IntegerParameter(params, "inactive-delete", 24 * 60 * 60 * 1000);

  final transient private IntegerParameter pMaxOpenTime = new IntegerParameter(params, "max-open-time", 30 * 60 * 1000);
  final transient private IntegerParameter pMaxInterval = new IntegerParameter(params, "max-interval", 60 * 1000);

  /** Aging */
  final transient private IntegerParameter pMaxAge = new IntegerParameter(params, "max-age", 2 * 24 * 60 * 60);

  /** Share MODE */
  final transient private IntegerParameter pShareMode = new IntegerParameter(params, "share-mode", LibSmb.SHARE_ALL);

  /** Whether to skip the position to EOF in the case of files not written on the position file. */
  final transient private BooleanParameter pSkipToEnd = new BooleanParameter(params, "skip-to-end", true);

  /** Whether to add the byte offset of a tracked line to the header */
  final transient private BooleanParameter pSetOffsetHeader = new BooleanParameter(params, "set-offset-header", false);
  /** Whether to include absolute path filename in a header. */
  final transient private BooleanParameter pSetPathHeader = new BooleanParameter(params, "set-path-header", false);
  /** Header in which to put absolute path filename. */
  final transient private StringParameter pSourceHeaderName = new StringParameter(params, "path-header-key", "source");
  final transient private StringParameter pOffsetHeaderName = new StringParameter(params, "offset-header-key", "byteoffset");
  final transient private BooleanParameter pCanBakeOff = new BooleanParameter(params, "bakeoff", true);

  /** Watchers */
  public static final String WATCHER_PREFIX = "watcher";
  final transient private ListParameter pWatchers = new ListParameter(params, TrackerManagerConfig.WATCHER_PREFIX + "s");

  /** Principal */
  public static final String PRINCIPAL_PREFIX = "principal";
  final transient private ListParameter pPrincipals = new ListParameter(params, TrackerManagerConfig.PRINCIPAL_PREFIX + "s", LibStr.EMPTY_STRINGS);

  public String positionFilePath;
  public int writePosInterval;

  public long inactiveFlush;
  public long inactiveClose;
  public long inactiveDelete;

  public long maxOpenTime;
  public long maxInterval;

  public int maxAge;
  public int shareMode;

  public String sourceHeaderName;
  public String offsetHeaderName;

  public boolean skipToEnd;
  public boolean canBakeOff;

  public Map<String, WatcherConfig> watcherConfigs = new HashMap<>();
  public transient Map<String, NtlmPasswordAuthenticator> principalConfigs = new HashMap<>();

  public TrackerManagerConfig(final ImmutableMap<String, String> config, final String prefix) {
    params.loadConfig(config, prefix);
    positionFilePath = pPositionFilePath.get().toString();
    skipToEnd = pSkipToEnd.get();
    sourceHeaderName = pSetOffsetHeader.get() ? pSourceHeaderName.get() : null;
    offsetHeaderName = pSetPathHeader.get() ? pOffsetHeaderName.get() : null;
    writePosInterval = pWritePosInternval.get();
    inactiveFlush = pInactiveFlush.get();
    inactiveClose = pInactiveClose.get();
    inactiveDelete = pInactiveDelete.get();
    maxOpenTime = pMaxOpenTime.get();
    maxInterval = pMaxInterval.get();
    maxAge = pMaxAge.get();
    shareMode = pShareMode.get();
    canBakeOff = pCanBakeOff.get();
    principalConfigs.clear();
    updatePrincipals(config, prefix, pPrincipals.get());
    watcherConfigs.clear();
    updateWatchers(config, prefix, pWatchers.get());
    Preconditions.checkState(!watcherConfigs.isEmpty(), "Watchers configuration is empty or invalid");
  }

  private void updatePrincipals(final ImmutableMap<String, String> context, final String prefix, final String[] principals) {
    if (principals == null) { return; }
    for (final String principalName : principals) {
      final String basePrefix = LibStr.concatenate(prefix, TrackerManagerConfig.PRINCIPAL_PREFIX, ".", principalName);
      final PrincipalConfig conf = new PrincipalConfig(context, basePrefix);
      final NtlmPasswordAuthenticator auth = new NtlmPasswordAuthenticator(conf.domain, conf.username, conf.password);
      principalConfigs.put(principalName, auth);
      TrackerManagerConfig.logger.debug(" {} Auth: {}", principalName, auth);
    }
  }

  private void updateWatchers(final ImmutableMap<String, String> context, final String prefix, final String[] watcherNames) {
    if (watcherNames == null) { return; }
    for (final String watcherName : watcherNames) {
      final String basePrefix = LibStr.concatenate(prefix, TrackerManagerConfig.WATCHER_PREFIX, ".", watcherName);
      final WatcherConfig watcherConfig = new WatcherConfig(watcherName, this, context, basePrefix);
      watcherConfigs.put(watcherName, watcherConfig);
      TrackerManagerConfig.logger.debug("Watcher: {} config: {}", watcherName, watcherConfig);
    }
  }

  public WatcherConfig getFileConfig(final String fileGroup) {
    return watcherConfigs.get(fileGroup);
  }

  @Override
  public String toString() {
    return GsonUtil.toJSON(this);
  }

}
