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
package net.eiroca.sysadm.flume.util.interceptors;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.sysadm.flume.api.IAction;
import net.eiroca.sysadm.flume.core.actions.Actions;
import net.eiroca.sysadm.flume.plugin.UltimateInterceptor;

public class HeadersConfig {

  public static final Logger logger = LoggerFactory.getLogger(UltimateInterceptor.class);

  private static final String CTX_HEADER_PREFIX = "header.";

  final private transient Parameters params = new Parameters();
  final private transient ListParameter pHeaders = new ListParameter(params, "headers", null);

  public List<IAction> headers = new ArrayList<>();

  public HeadersConfig(final String name, final ImmutableMap<String, String> config) {
    configure(config);
    HeadersConfig.logger.info("Config for {}: {}", name, this);
  }

  protected void configure(final ImmutableMap<String, String> config) {
    HeadersConfig.logger.trace("Starting config");
    params.loadConfig(config, null);
    Actions.load(pHeaders.get(), config, HeadersConfig.CTX_HEADER_PREFIX, headers);
  }

  @Override
  public String toString() {
    return new Gson().toJson(this).toString();
  }

}
