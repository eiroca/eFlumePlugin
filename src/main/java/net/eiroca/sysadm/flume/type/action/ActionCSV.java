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
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.Action;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.core.util.MappingCSVData;

public class ActionCSV extends Action {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pMatchHeader = new StringParameter(params, "match", "%{hostname}");
  final private transient StringParameter pMatchDefault = new StringParameter(params, "match-default", "*");
  final private transient StringParameter pCSVFile = new StringParameter(params, "mapping");
  final private transient StringParameter pCSVSeparator = new StringParameter(params, "csv-separator-char", ",");

  public String matchHeaderKey;
  public String headerDefault;

  private MappingCSVData mapping;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    matchHeaderKey = pMatchHeader.get();
    headerDefault = pMatchDefault.get();
    mapping = new MappingCSVData(pCSVFile.get(), pCSVSeparator.get());
    ActionCSV.logger.debug("CSV Field config: {}", this);
  }

  @Override
  public void run(final Map<String, String> headers, final String body) {
    Map<String, String> extraHeaderInfo = null;
    String keyVal = MacroExpander.expand(matchHeaderKey, headers, body);
    extraHeaderInfo = mapping.get(keyVal);
    if (extraHeaderInfo == null) {
      ActionCSV.logger.trace("{} not found using {}", keyVal, headerDefault);
      keyVal = headerDefault;
      extraHeaderInfo = mapping.get(keyVal);
    }
    ActionCSV.logger.trace("{} headers -> {}", keyVal, extraHeaderInfo);
    if (extraHeaderInfo != null) {
      for (final Map.Entry<String, String> entry : extraHeaderInfo.entrySet()) {
        final String key = entry.getKey();
        final String val = entry.getValue();
        setHeader(headers, body, key, val);
      }
    }
  }
}
