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
package net.eiroca.sysadm.flume.type.action;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.core.util.HeaderAction;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.util.sessions.SessionManager;

public class HeaderSession extends HeaderAction {

  final private transient StringParameter pSessionKey = new StringParameter(params, "key");
  final private transient StringParameter pSessionValidate = new StringParameter(params, "validate", null);
  final private transient LongParameter pTTL = new LongParameter(params, "TTL", 15 * 60);
  final private transient StringParameter pTimestamp = new StringParameter(params, "timestamp", "%{timestamp}");

  protected SessionManager manager;

  public String key;
  public String validate;
  public String timestamp;
  public long ttl;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    final String value = config.get(prefix);
    final String sessionPrefix = LibStr.concatenate(prefix, ".");
    params.loadConfig(config, sessionPrefix);
    if (value == null) {
      force = true;
    }
    else {
      force = new Boolean(String.valueOf(value));
    }
    key = pSessionKey.get();
    validate = pSessionValidate.get();
    timestamp = pTimestamp.get();
    ttl = pTTL.get() * 1000;
    if (ttl < 1) {
      ttl = 1;
    }
    manager = SessionManager.getManager(name);
    manager.setTTL(ttl);
  }

  @Override
  public String getValue(final Map<String, String> headers, final String body) {
    final String aKey = MacroExpander.expand(key, headers, body);
    MacroExpander.expand(validate, headers, body);
    final String aTimeStamp = MacroExpander.expand(timestamp, headers, body);
    Helper.getLong(aTimeStamp, System.currentTimeMillis());
    // final Session s = manager.find(aKey, timeStamp, aValidate);
    // return s.ID();
    return Integer.toHexString(aKey.hashCode());
  }

}
