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
package net.eiroca.sysadm.flume.plugin;

import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.actions.Actions;
import net.eiroca.sysadm.flume.core.util.LicenseCheck;
import net.eiroca.sysadm.flume.util.interceptors.HeadersConfig;

public class HeadersInterceptor implements Interceptor {

  transient private static final Logger logger = Logs.getLogger();
  HeadersConfig config;

  public HeadersInterceptor(final HeadersConfig defaultConfig) {
    LicenseCheck.runCheck();
    config = defaultConfig;
    HeadersInterceptor.logger.debug("Headers config: {}", config);
  }

  @Override
  public void initialize() {
    HeadersInterceptor.logger.debug("Initialize {}...", this);
  }

  @Override
  public Event intercept(final Event event) {
    HeadersInterceptor.logger.trace("Intercept Event: {}", event);
    try {
      final Map<String, String> headers = event.getHeaders();
      Actions.execute(config.headers, headers, "");
    }
    catch (final Exception e) {
      HeadersInterceptor.logger.error("Interceptor unexpexted error: ", e);
    }
    HeadersInterceptor.logger.debug("Interceptor header event: {}", event);
    return event;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    if (events == null) { return events; }
    HeadersInterceptor.logger.debug("Interception {} event(s)", events.size(), this);
    long elapsed = System.currentTimeMillis();
    for (final Event e : events) {
      intercept(e);
    }
    elapsed = (System.currentTimeMillis() - elapsed);
    HeadersInterceptor.logger.debug("Headers kept: {} elapsed: {} ms", events.size(), elapsed);
    return events;
  }

  @Override
  public void close() {
    HeadersInterceptor.logger.debug("Close {}...", this);
  }

  public static class Builder implements Interceptor.Builder {

    HeadersConfig config;

    @Override
    public void configure(final Context context) {
      config = new HeadersConfig(getClass().getName(), context.getParameters());
    }

    @Override
    public Interceptor build() {
      return new HeadersInterceptor(config);
    }

  }

}
