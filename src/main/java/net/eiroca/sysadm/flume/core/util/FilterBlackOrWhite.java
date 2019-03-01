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

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.parameter.BooleanParameter;
import net.eiroca.library.parameter.IntegerParameter;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.library.system.Logs;

abstract public class FilterBlackOrWhite extends Filter {

  transient protected static final Logger logger = Logs.getLogger();

  final private transient StringParameter pFilterMatch = new StringParameter(params, "match", "%()");
  final private transient IntegerParameter pFilterMatchLimit = new IntegerParameter(params, "size-limit", -1);
  final private transient BooleanParameter pIgnoreCase = new BooleanParameter(params, "ignore-case", false);
  final private transient BooleanParameter pFilterAcceptTT = new BooleanParameter(params, "accept.TT", true);
  final private transient BooleanParameter pFilterAcceptTF = new BooleanParameter(params, "accept.TF", true);
  final private transient BooleanParameter pFilterAcceptFT = new BooleanParameter(params, "accept.FT", false);
  final private transient BooleanParameter pFilterAcceptFF = new BooleanParameter(params, "accept.FF", true);

  protected String filterMatch;
  protected int filterMatchLimit;
  protected boolean ignoreCase;
  protected HashMap<String, Boolean> filterAcceptRules = new HashMap<>();

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    filterAcceptRules.clear();
    filterMatch = pFilterMatch.get();
    filterMatchLimit = pFilterMatchLimit.get();
    ignoreCase = pIgnoreCase.get();
    filterAcceptRules.put("TT", pFilterAcceptTT.get());
    filterAcceptRules.put("TF", pFilterAcceptTF.get());
    filterAcceptRules.put("FT", pFilterAcceptFT.get());
    filterAcceptRules.put("FF", pFilterAcceptFF.get());
  }

  @Override
  public boolean accept(final Map<String, String> headers, final String body) {
    if (LibStr.isEmptyOrNull(body)) { return acceptNullBody; }
    if (filterMatch == null) { return true; }
    String match = LibStr.limit(MacroExpander.expand(filterMatch, headers, body), filterMatchLimit);
    if (ignoreCase) {
      match = match.toLowerCase();
    }
    FilterBlackOrWhite.logger.trace(getClass().getCanonicalName() + ": " + match);
    final boolean inWhiteList = isInWhiteList(match);
    final boolean inBlackList = isInBlackList(match);
    final String rule = (inWhiteList ? "T" : "F") + (inBlackList ? "T" : "F");
    final Boolean accept = filterAcceptRules.get(rule);
    final boolean accepted = (accept != null) ? accept.booleanValue() : false;
    FilterBlackOrWhite.logger.trace("FILTERING {} : {}", rule, accept);
    return accepted;
  }

  abstract public boolean isInWhiteList(String match);

  abstract public boolean isInBlackList(String match);

}
