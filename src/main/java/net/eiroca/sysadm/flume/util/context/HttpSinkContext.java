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
package net.eiroca.sysadm.flume.util.context;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import net.eiroca.sysadm.flume.core.util.GenericSinkContext;
import net.eiroca.sysadm.flume.plugin.HttpSink;

public class HttpSinkContext extends GenericSinkContext<HttpSink> {

  CloseableHttpClient httpclient = null;
  public boolean shouldRollback = false;
  public boolean shouldBackeoff = false;

  public HttpSinkContext(final HttpSink owner) {
    super(owner);
  }

  public CloseableHttpClient getHttpClient() {
    if (httpclient == null) {
      final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
      requestConfigBuilder.setConnectionRequestTimeout(owner.requestTimeout);
      requestConfigBuilder.setConnectTimeout(owner.connectTimeout);
      requestConfigBuilder.setSocketTimeout(owner.socketTimeout);
      final HttpClientBuilder builder = HttpClientBuilder.create().setDefaultRequestConfig(requestConfigBuilder.build());
      httpclient = builder.build();
    }
    return httpclient;
  }

}
