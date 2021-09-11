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
package net.eiroca.sysadm.flume.type.converter;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.converters.Converter;

public class StringConverter extends Converter<String> {

  final private transient StringParameter pDefault = new StringParameter(params, "default", "");
  final private transient StringParameter pNullReplace = new StringParameter(params, "null-replace", "-");
  final private transient StringParameter pFormat = new StringParameter(params, "format", "%s");
  final private transient BooleanParameter pRemoveQuote = new BooleanParameter(params, "remove-quote", true);
  final private transient StringParameter pQuote = new StringParameter(params, "quote", "\"");
  final private transient BooleanParameter pForceUpper = new BooleanParameter(params, "force-uppercase", false);
  final private transient BooleanParameter pForceLower = new BooleanParameter(params, "force-lowercase", false);
  final private transient StringParameter pRemovePrefix = new StringParameter(params, "remove-prefix", null);
  final private transient StringParameter pForcePrefix = new StringParameter(params, "force-prefix", null);
  final private transient StringParameter pRemoveSuffix = new StringParameter(params, "remove-suffix", null);
  final private transient StringParameter pForceSuffix = new StringParameter(params, "force-suffix", null);
  final private transient IntegerParameter pBeginIndex = new IntegerParameter(params, "begin-index", false);
  final private transient IntegerParameter pEndIndex = new IntegerParameter(params, "end-index", false);

  protected String defVal;
  protected String quote;
  protected boolean removeQuote;
  protected String nullReplace;
  protected String format;
  protected boolean forceUpper;
  protected boolean forceLower;
  protected String removePrefix;
  protected String forcePrefix;
  protected String removeSuffix;
  protected String forceSuffix;
  protected Integer beginIndex;
  protected Integer endIndex;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    defVal = pDefault.get();
    removeQuote = pRemoveQuote.get();
    quote = pQuote.get();
    nullReplace = pNullReplace.get();
    format = pFormat.get();
    forceLower = pForceLower.get();
    forceUpper = pForceUpper.get();
    removePrefix = pRemovePrefix.get();
    forcePrefix = pForcePrefix.get();
    beginIndex = pBeginIndex.get();
    endIndex = pEndIndex.get();
    removeSuffix = pRemoveSuffix.get();
    forceSuffix = pForceSuffix.get();
  }

  @Override
  public String doConvert(String value) {
    if (value == null) { return defVal; }
    if (removeQuote && (value.length() > 2)) {
      if (value.startsWith(quote) && value.endsWith(quote)) {
        value = value.substring(quote.length(), value.length() - quote.length());
      }
    }
    if ((nullReplace != null) && value.equals(nullReplace)) { return defVal; }
    if (forceLower) {
      value = value.toLowerCase();
    }
    if (forceUpper) {
      value = value.toUpperCase();
    }
    if ((removePrefix != null) && (value.startsWith(removePrefix))) {
      value = value.substring(removePrefix.length(), value.length());
    }
    if ((removeSuffix != null) && (value.endsWith(removeSuffix))) {
      value = value.substring(0, value.length() - removeSuffix.length());
    }
    if ((forcePrefix != null) && (!value.startsWith(forcePrefix))) {
      value = forcePrefix + value;
    }
    if ((forceSuffix != null) && (!value.endsWith(forceSuffix))) {
      value = value + forceSuffix;
    }
    if ((beginIndex != null) || (endIndex != null)) {
      final int len = value.length();
      int iBegin = beginIndex != null ? beginIndex.intValue() : 0;
      int iEnd = endIndex != null ? endIndex.intValue() : len;
      if (iBegin < 0) {
        iBegin = len + iBegin;
      }
      if (iEnd < 0) {
        iEnd = len + iEnd;
      }
      if (iBegin < 0) {
        iBegin = 0;
      }
      else if (iBegin > len) {
        iBegin = len;
      }
      if (iEnd < 0) {
        iEnd = 0;
      }
      else if (iEnd > len) {
        iEnd = len;
      }
      value = value.substring(iBegin, iEnd);
    }
    if (format != null) {
      value = String.format(format, value);
    }
    return value;
  }

}
