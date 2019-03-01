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
package net.eiroca.sysadm.flume.core.util;

import java.util.Map;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.parameter.BooleanParameter;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IAction;
import net.eiroca.sysadm.flume.api.IEventFilter;
import net.eiroca.sysadm.flume.api.IStringExtractor;
import net.eiroca.sysadm.flume.core.Filters;

abstract public class Action extends ConfigurableObject implements IAction {

  transient protected static final Logger logger = Logs.getLogger();

  private static final String CTX_FITLER_PREFIX = "condition.";

  final private transient StringParameter pDefaultNull = new StringParameter(params, "default-null", "");
  final private transient BooleanParameter pForce = new BooleanParameter(params, "force", false);
  final private transient BooleanParameter pExpand = new BooleanParameter(params, "expand", true);
  final private transient StringParameter pFilterType = new StringParameter(params, Action.CTX_FITLER_PREFIX + "type", null);
  final private transient StringParameter pFilterMatch = new StringParameter(params, Action.CTX_FITLER_PREFIX + "match", null);
  final private transient BooleanParameter pSilentError = new BooleanParameter(params, "silent-error", true);

  protected String defaultNull = "";
  protected boolean force = true;
  protected boolean expand = true;
  protected IEventFilter filter;
  protected boolean silentError = true;

  @Override
  public boolean isConfigurable() {
    return true;
  }

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    final String value = config.get(prefix);
    if (value != null) {
      Action.logger.warn("Invalid configuration {} must be null", prefix);
    }
    final String sessionPrefix = LibStr.concatenate(prefix, ".");
    params.laodConfig(config, sessionPrefix);
    defaultNull = pDefaultNull.get();
    force = pForce.get();
    expand = pExpand.get();
    silentError = pSilentError.get();
    filter = Filters.buildFilter(config, LibStr.concatenate(sessionPrefix, Action.CTX_FITLER_PREFIX), pFilterType.get(), pFilterMatch.get());
  }

  protected void setHeader(final Map<String, String> headers, final String body, final String name, final IStringExtractor extractor) {
    final boolean isSet = headers.containsKey(name);
    if ((!isSet) || force) {
      String value = extractor.getValue(headers, body);
      if (expand) {
        value = MacroExpander.expand(value, headers, body);
      }
      setHeader(headers, name, value);
    }
  }

  protected void setHeader(final Map<String, String> headers, final String name, String value) {
    if (value == null) {
      value = defaultNull;
    }
    Action.logger.trace(LibStr.concatenate("Header Set ", name, " = ", value));
    headers.put(name, value);
  }

  @Override
  final public void execute(final Map<String, String> headers, final String body) {
    Action.logger.trace("Executing {}", getName());
    if ((filter == null) || filter.accept(headers, body)) {
      Action.logger.trace("Running {}", getName());
      try {
        run(headers, body);
      }
      catch (final Exception e) {
        if (!silentError) { throw e; }
        Action.logger.info("Action {} ignoring error", getName(), e);
      }
    }
  }

  abstract public void run(Map<String, String> headers, String body);

}
