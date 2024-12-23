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

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;

public class HttpQueryExtractor extends SpacerExtractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pPrefix = new StringParameter(params, "prefix", "httpquery_");
  final private transient StringParameter pCharsetName = new StringParameter(params, "charset", "UTF-8");
  final private transient ListParameter pParamsList = new ListParameter(params, "params", null);
  final private transient BooleanParameter pMode = new BooleanParameter(params, "params-remove", true);
  final private transient BooleanParameter pCaseSensitive = new BooleanParameter(params, "case-sensitive", true);

  protected String prefix;
  protected Charset charset;
  protected boolean isRemove;
  protected boolean caseSensitive;
  protected Set<String> paramList;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    HttpQueryExtractor.logger.trace("config {}: {}", prefix, config);
    this.prefix = pPrefix.get();
    charset = Charset.forName(pCharsetName.get());
    caseSensitive = pCaseSensitive.get();
    isRemove = pMode.get();
    paramList = new TreeSet<>();
    String[] tempRemoveList;
    tempRemoveList = pParamsList.get();
    if (tempRemoveList != null) {
      for (final String element : tempRemoveList) {
        paramList.add(element);
      }
    }
  }

  @Override
  public Tags getTags(final String value) {
    if (value == null) { return null; }
    HttpQueryExtractor.logger.debug(getClass().getCanonicalName() + " source: " + value);
    final Tags result = new Tags();
    List<NameValuePair> params;
    params = URLEncodedUtils.parse(value, charset);
    for (final NameValuePair param : params) {
      String name = param.getName();
      if (!caseSensitive) name = name.toLowerCase();
      boolean inList = paramList.contains(name);
      boolean add = (!inList && isRemove) || (inList && !isRemove);
      if (add) {
        final String val = param.getValue();
        result.add(prefix + name, val);
      }
    }
    return result;
  }

}
