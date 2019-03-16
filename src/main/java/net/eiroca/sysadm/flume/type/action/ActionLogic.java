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
package net.eiroca.sysadm.flume.type.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IAction;
import net.eiroca.sysadm.flume.core.Actions;
import net.eiroca.sysadm.flume.core.util.Action;

public class ActionLogic extends Action {

  transient private static final Logger logger = Logs.getLogger();

  private static final String CTX_ACTION_PREFIX = "header";

  final private transient ListParameter pLogicActions = new ListParameter(params, "headers", null);

  protected List<IAction> actions = new ArrayList<>();

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    Actions.load(pLogicActions.get(), config, LibStr.concatenate(prefix, (prefix != null ? "." : null), ActionLogic.CTX_ACTION_PREFIX, "."), actions);
    ActionLogic.logger.debug("{} config: {}", getName(), this);
  }

  @Override
  public void run(final Map<String, String> headers, final String body) {
    ActionLogic.logger.trace("Processing ActionLogic on: {}", getName());
    Actions.execute(actions, headers, body);
  }

}
