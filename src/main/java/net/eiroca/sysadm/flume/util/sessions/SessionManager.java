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
package net.eiroca.sysadm.flume.util.sessions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.eiroca.ext.library.service.LibService;

public class SessionManager {

  public static final Logger logger = LoggerFactory.getLogger(SessionManager.class);

  private static final Map<String, SessionManager> managers = new HashMap<>();
  private static long checkInterval = 10000;

  private long ttl = 0;

  private final Map<String, Session> sessions = new HashMap<>();

  static {
    LibService.startService(new SessionEraseTask(), SessionManager.checkInterval, SessionManager.checkInterval);
  }

  private SessionManager() {
  }

  public static synchronized SessionManager getManager(final String id) {
    SessionManager result = SessionManager.managers.get(id);
    if (result == null) {
      result = new SessionManager();
      SessionManager.managers.put(id, result);
    }
    return result;
  }

  public synchronized Session find(final String key, final long timestamp, final String validating) {
    Session result = sessions.get(key);
    SessionManager.logger.debug("Session: {} -> {}", key, result);
    if (result != null) {
      result = result.validate(timestamp, validating);
      SessionManager.logger.trace("Session: validated {} -> {}", key, result);
    }
    if (result == null) {
      SessionManager.logger.trace("Session: new {}", key);
      result = new Session(key, timestamp, validating);
      if (ttl > 0) {
        result.TTL = ttl;
      }
      sessions.put(key, result);
    }
    else {
      result.update(timestamp);
    }
    return result;
  }

  public synchronized void cleanup(final long timestamp) {
    final List<String> invalidKey = new ArrayList<>();
    final int size = sessions.size();
    for (final Entry<String, Session> e : sessions.entrySet()) {
      if (!e.getValue().isValid(timestamp)) {
        invalidKey.add(e.getKey());
      }
    }
    if (invalidKey.size() > 0) {
      for (final String key : invalidKey) {
        sessions.remove(key);
      }
      SessionManager.logger.info("Clueanup {} -> {}", size, sessions.size());
    }
  }

  public static void cleanup() {
    for (final Entry<String, SessionManager> manager : SessionManager.managers.entrySet()) {
      SessionManager.logger.trace("Starting clueanup {}", manager.getKey());
      final SessionManager sm = manager.getValue();
      sm.cleanup(System.currentTimeMillis());
    }
  }

  public void setTTL(final long ttl) {
    this.ttl = ttl;
  }

}
