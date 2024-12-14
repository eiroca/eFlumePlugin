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
package net.eiroca.sysadm.flume.type.extractor;

import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.extractors.Extractors;

public class HttpRequestLineExtractor extends SpacerExtractor {

  static {
    Extractors.registry.addEntry("httprequest", HttpRequestLineExtractor.class.getName());
  }

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pMethodName = new StringParameter(params, "method-name", "CSmethod");
  final private transient StringParameter pUriName = new StringParameter(params, "uri-name", "CSuri");
  final private transient StringParameter pQueryName = new StringParameter(params, "method-name", "CSuriquery");
  final private transient StringParameter pProtocolName = new StringParameter(params, "method-name", "CSprotocol");

  public String methodName;
  public String uriName;
  public String queryName;
  public String protocolName;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    HttpRequestLineExtractor.logger.trace("config {}: {}", prefix, config);
    methodName = pMethodName.get();
    uriName = pUriName.get();
    queryName = pQueryName.get();
    protocolName = pProtocolName.get();
  }

  @Override
  public Tags getTags(final String value) {
    if (value == null) { return null; }
    HttpRequestLineExtractor.logger.debug(getClass().getCanonicalName() + " source: " + value);
    Tags result = new Tags();
    if (!splitLine(result, value)) {
      result = null;
    }
    return result;
  }

  private enum LineStates {
    IN_METHOD, IN_URI, IN_QUERY, IN_PROTOCOL
  }

  public boolean splitLine(final Tags t, final String row) {
    if (row == null) { return false; }
    final int len = row.length();
    int pos = 0;
    int start = 0;
    LineStates state = LineStates.IN_METHOD;
    while (pos < len) {
      final char ch = row.charAt(pos);
      switch (state) {
        case IN_METHOD:
          if (ch == ' ') {
            t.add(methodName, row.substring(start, pos));
            state = LineStates.IN_URI;
            start = pos + 1;
          }
          break;
        case IN_URI:
          if ((ch == ' ') || (ch == '?')) {
            t.add(uriName, row.substring(start, pos));
            start = pos + 1;
            state = (ch == ' ') ? LineStates.IN_PROTOCOL : LineStates.IN_QUERY;
          }
          break;
        case IN_QUERY:
          if (ch == ' ') {
            t.add(queryName, row.substring(start, pos));
            state = LineStates.IN_PROTOCOL;
            start = pos + 1;
          }
          break;
        case IN_PROTOCOL:
          pos = len;
          break;
      }
      pos++;
    }
    if (state != LineStates.IN_PROTOCOL) { return false; }
    t.add(protocolName, row.substring(start, pos));
    return true;
  }

}
