/**
 *
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.GenericSink;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.util.context.HttpSinkContext;

public class HttpSink extends GenericSink<HttpSinkContext> {

  private static final String HEADER_CONTENT_TYPE = "Content-type";
  private static final String HEADER_ACCEPT = "Accept";

  private static final String INCREMENT_METRICS = "increment-metrics";
  private static final String ROLLBACK = "rollback";
  private static final String BACKOFF = "backoff";

  final private StringParameter pEncoding = new StringParameter(params, "encoding", "utf-8");

  /** Server URL */
  final StringParameter pEndPoint = new StringParameter(params, "url");
  final StringParameter pMethod = new StringParameter(params, "method", "POST");
  /** connection timeout when calling endpoint. */
  final IntegerParameter pConnectTimeout = new IntegerParameter(params, "connect-timeout", 5000);
  /** request timeout when calling endpoint. */
  final IntegerParameter pRequestTimeout = new IntegerParameter(params, "request-timeout", 5000);
  /** socket timeout when calling endpoint. */
  final IntegerParameter pSocketTimeout = new IntegerParameter(params, "socket-timeout", 5000);
  /** HTTP content type header. */
  final StringParameter pContentTypeHeader = new StringParameter(params, "content-type", "text/plain");
  /** HTTP accept header. */
  final StringParameter pAcceptHeader = new StringParameter(params, "accept", "text/plain");

  final BooleanParameter pDefaultBackoff = new BooleanParameter(params, "default-backoff", true);
  final BooleanParameter pDefaultRollback = new BooleanParameter(params, "default-rollback", true);
  final BooleanParameter pDefaultIncrementMetrics = new BooleanParameter(params, "default-increment-metrics", false);

  protected String encoding;

  /** Endpoint URL to POST events to. */
  String endPoint;
  String method;

  /** Actual connection timeout value in use. */
  public int connectTimeout;
  /** Actual request timeout value in use. */
  public int requestTimeout;
  public int socketTimeout;
  /** Actual content type header value in use. */
  String contentTypeHeader;
  /** Actual accept header value in use. */
  String acceptHeader;
  /** Backoff value to use if a specific override is not defined. */
  private boolean defaultBackoff;
  /** Rollback value to use if a specific override is not defined. */
  private boolean defaultRollback;
  /** Increment metrics value to use if a specific override is not defined. */
  private boolean defaultIncrementMetrics;

  /**
   * Holds all overrides for backoff. The key is a string of the format "500" or "5XX", and the
   * value is the backoff value to use for the individual code, or code range.
   */
  private final HashMap<String, Boolean> backoffOverrides = new HashMap<>();

  /**
   * Holds all overrides for rollback. The key is a string of the format "500" or "5XX", and the
   * value is the rollback value to use for the individual code, or code range.
   */
  private final HashMap<String, Boolean> rollbackOverrides = new HashMap<>();

  /**
   * Holds all overrides for increment metrics. The key is a string of the format "500" or "5XX",
   * and the value is the increment metrics value to use for the individual code, or code range.
   */
  private final HashMap<String, Boolean> incrementMetricsOverrides = new HashMap<>();

  @Override
  public void configure(final Context context) {
    super.configure(context);
    encoding = pEncoding.get();
    endPoint = pEndPoint.get();
    method = pMethod.get();
    connectTimeout = pConnectTimeout.get();
    if (connectTimeout <= 0) { throw new IllegalArgumentException("Connect timeout must be a non-zero and positive"); }
    requestTimeout = pRequestTimeout.get();
    if (requestTimeout <= 0) { throw new IllegalArgumentException("Request timeout must be a non-zero and positive"); }
    socketTimeout = pSocketTimeout.get();
    if (socketTimeout <= 0) { throw new IllegalArgumentException("Socket timeout must be a non-zero and positive"); }
    acceptHeader = pAcceptHeader.get();
    contentTypeHeader = pContentTypeHeader.get();
    defaultBackoff = pDefaultBackoff.get();
    defaultRollback = pDefaultRollback.get();
    defaultIncrementMetrics = pDefaultIncrementMetrics.get();
    GenericSink.logger.info("Read endpoint URL from configuration: " + endPoint);
    GenericSink.logger.info("Using connect timeout: " + connectTimeout);
    GenericSink.logger.info("Using request timeout: " + requestTimeout);
    GenericSink.logger.info("Using socket timeout: " + socketTimeout);
    GenericSink.logger.info("Using Accept header value: " + acceptHeader);
    GenericSink.logger.info("Using Content-Type header value: " + contentTypeHeader);
    GenericSink.logger.info("Channel backoff by default is " + defaultBackoff);
    GenericSink.logger.info("Transaction rollback by default is " + defaultRollback);
    GenericSink.logger.info("Incrementing metrics by default is " + defaultIncrementMetrics);
    parseConfigOverrides(HttpSink.BACKOFF, context, backoffOverrides);
    parseConfigOverrides(HttpSink.ROLLBACK, context, rollbackOverrides);
    parseConfigOverrides(HttpSink.INCREMENT_METRICS, context, incrementMetricsOverrides);
  }

  @Override
  public HttpSinkContext processBegin() throws Exception {
    return new HttpSinkContext(this);
  }

  @Override
  protected EventStatus process(final HttpSinkContext context, final Event event, final Map<String, String> headers, final String body) throws Exception {
    EventStatus result = EventStatus.OK;
    final CloseableHttpClient httpClient = context.getHttpClient();
    final String url = MacroExpander.expand(endPoint, headers, body);
    final StringBuilder reqData = new StringBuilder();
    if (body != null) {
      reqData.append(String.valueOf(body));
    }
    GenericSink.logger.trace("Request URL : " + url);
    GenericSink.logger.trace("Request body: " + reqData);
    final StringEntity entity = new StringEntity(reqData.toString(), encoding);
    final HttpEntityEnclosingRequestBase httpMethod = getMethod(method, url);
    httpMethod.setEntity(entity);
    httpMethod.setHeader(HttpSink.HEADER_ACCEPT, acceptHeader);
    httpMethod.setHeader(HttpSink.HEADER_CONTENT_TYPE, contentTypeHeader);
    CloseableHttpResponse response = null;
    response = httpClient.execute(httpMethod);
    final int httpStatusCode = response.getStatusLine().getStatusCode();
    final HttpEntity entity2 = response.getEntity();
    try {
      EntityUtils.consume(entity2);
    }
    catch (final IOException e) {
    }
    response.close();
    GenericSink.logger.debug("Got status code: " + httpStatusCode);
    if (httpStatusCode >= HttpURLConnection.HTTP_BAD_REQUEST) {
      GenericSink.logger.debug("bad request");
    }
    GenericSink.logger.debug("Response processed and closed");
    final String httpStatusString = String.valueOf(httpStatusCode);
    context.shouldRollback = findOverrideValue(httpStatusString, rollbackOverrides, defaultRollback);
    context.shouldBackeoff = findOverrideValue(httpStatusString, backoffOverrides, defaultBackoff);
    final boolean shouldIncrementMetrics = findOverrideValue(httpStatusString, incrementMetricsOverrides, defaultIncrementMetrics);
    if (context.shouldBackeoff || context.shouldRollback) {
      result = shouldIncrementMetrics ? EventStatus.STOP : EventStatus.ERROR;
    }
    else {
      result = shouldIncrementMetrics ? EventStatus.OK : EventStatus.IGNORED;
    }
    return result;
  }

  @Override
  public ProcessStatus processEnd(final HttpSinkContext context) throws Exception {
    ProcessStatus result;
    final int code = (context.shouldRollback ? 2 : 0) + (context.shouldBackeoff ? 1 : 0);
    switch (code) {
      case 1: {
        result = ProcessStatus.BAKEOFF;
        break;
      }
      case 2: {
        result = ProcessStatus.ROLLBACK;
        break;
      }
      case 3: {
        result = ProcessStatus.FAIL;
        break;
      }
      default: {
        result = ProcessStatus.COMMIT;
        break;
      }
    }
    return result;
  }

  /**
   * Reads a set of override values from the context configuration and stores the results in the Map
   * provided.
   *
   * @param propertyName the prefix of the config property names
   * @param context the context to use to read config properties
   * @param override the override Map to store results in
   */
  private void parseConfigOverrides(final String propertyName, final Context context, final Map<String, Boolean> override) {
    final ImmutableMap<String, String> config = context.getSubProperties(propertyName + ".");
    if (config != null) {
      for (final Map.Entry<String, String> value : config.entrySet()) {
        GenericSink.logger.info(String.format("Read %s value for status code %s as %s", propertyName, value.getKey(), value.getValue()));
        if (override.containsKey(value.getKey())) {
          GenericSink.logger.warn(String.format("Ignoring duplicate config value for %s.%s", propertyName, value.getKey()));
        }
        else {
          override.put(value.getKey(), Boolean.valueOf(value.getValue()));
        }
      }
    }
  }

  /**
   * Queries the specified override map to find the most appropriate value. The most specific match
   * is found.
   *
   * @param statusCode the String representation of the HTTP status code
   * @param overrides the map of status code overrides
   * @param defaultValue the default value to use if no override is configured
   *
   * @return the value of the most specific match to the given status code
   */
  private boolean findOverrideValue(final String statusCode, final HashMap<String, Boolean> overrides, final boolean defaultValue) {
    Boolean overrideValue = overrides.get(statusCode);
    if (overrideValue == null) {
      overrideValue = overrides.get(statusCode.substring(0, 1) + "XX");
      if (overrideValue == null) {
        overrideValue = defaultValue;
      }
    }
    return overrideValue;
  }

  public HttpEntityEnclosingRequestBase getMethod(final String method, final String url) {
    if ("POST".equals(method)) { return new HttpPost(url); }
    if ("PUT".equals(method)) { return new HttpPut(url); }
    return null;
  }

}
