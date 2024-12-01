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
package net.eiroca.sysadm.flume.core.actions;

import java.util.List;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IAction;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.action.ActionCSV;
import net.eiroca.sysadm.flume.type.action.ActionDelete;
import net.eiroca.sysadm.flume.type.action.ActionExtractor;
import net.eiroca.sysadm.flume.type.action.ActionLogic;
import net.eiroca.sysadm.flume.type.action.HeaderRegEx;
import net.eiroca.sysadm.flume.type.action.HeaderSequence;
import net.eiroca.sysadm.flume.type.action.HeaderSession;
import net.eiroca.sysadm.flume.type.action.HeaderSet;
import net.eiroca.sysadm.flume.type.action.HeaderTimestamp;
import net.eiroca.sysadm.flume.type.action.HeaderUUID;

public class Actions {

  public static final Registry<String> registry = new Registry<String>();

  private static final String HEADER_ACTION_SUFFIX_ = ".type";
  private static final String HEADER_ACTION_SUFFIX_ALT = ".action";

  static {
    Actions.registry.addEntry(HeaderSet.class.getName());
    Actions.registry.addEntry("set", HeaderSet.class.getName());
    Actions.registry.addEntry("delete", ActionDelete.class.getName());
    Actions.registry.addEntry("sequence", HeaderSequence.class.getName());
    Actions.registry.addEntry("timestamp", HeaderTimestamp.class.getName());
    Actions.registry.addEntry("agent", HeaderSet.class.getName());
    Actions.registry.addEntry("csv", ActionCSV.class.getName());
    Actions.registry.addEntry("regex", HeaderRegEx.class.getName());
    Actions.registry.addEntry("logid", HeaderUUID.class.getName());
    Actions.registry.addEntry("id", HeaderUUID.class.getName());
    Actions.registry.addEntry("session", HeaderSession.class.getName());
    Actions.registry.addEntry("euid", HeaderUUID.class.getName());
    Actions.registry.addEntry("uuid", HeaderUUID.class.getName());
    Actions.registry.addEntry("tuid", HeaderSession.class.getName());
    Actions.registry.addEntry("suid", HeaderSession.class.getName());
    Actions.registry.addEntry("parser", ActionExtractor.class.getName());
    Actions.registry.addEntry("parsing", ActionExtractor.class.getName());
    Actions.registry.addEntry("extractor", ActionExtractor.class.getName());
    Actions.registry.addEntry("logic", ActionLogic.class.getName());
    Actions.registry.addEntry("condition", ActionLogic.class.getName());
  }

  public static IAction build(final String name, final String type, final ImmutableMap<String, String> config, final String prefix) {
    return (IAction)FlumeHelper.buildIConfigurable(name, Actions.registry.value(type), config, prefix);
  }

  public static IAction build(final String name, final ImmutableMap<String, String> config, final String prefix) {
    final String key = LibStr.concatenate(prefix, name);
    String actionType = config.get(key + Actions.HEADER_ACTION_SUFFIX_);
    if (actionType == null) {
      actionType = config.get(key + Actions.HEADER_ACTION_SUFFIX_ALT);
    }
    if (!Actions.registry.isValid(actionType)) {
      if (Actions.registry.isValid(name)) {
        actionType = name;
      }
      else {
        actionType = Actions.registry.defaultName();
      }
    }
    final IAction action = Actions.build(name, actionType, config, key);
    return action;
  }

  public static void execute(final List<IAction> commands, final Map<String, String> headers, final String body) {
    for (final IAction command : commands) {
      command.execute(headers, body);
    }
  }

  public static void load(final String[] actionNames, final ImmutableMap<String, String> config, final String prefix, final List<IAction> actions) {
    actions.clear();
    if (actionNames != null) {
      for (final String name : actionNames) {
        final IAction header = Actions.build(name, config, prefix);
        if (header != null) {
          actions.add(header);
        }
      }
    }
  }

}
