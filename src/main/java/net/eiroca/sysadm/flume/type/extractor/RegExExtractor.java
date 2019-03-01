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
package net.eiroca.sysadm.flume.type.extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.parameter.BooleanParameter;
import net.eiroca.library.parameter.IntegerParameter;
import net.eiroca.library.parameter.RegExParameter;
import net.eiroca.library.system.LibRegEx;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.Extractor;

/**
 * Converter that simply returns the passed in value
 */
public class RegExExtractor extends Extractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient RegExParameter pRegEx = new RegExParameter(params, "pattern");
  final private transient BooleanParameter pRegExExtractNamed = new BooleanParameter(params, "extract-named", true);
  final private transient IntegerParameter pRegExLimit = new IntegerParameter(params, "size-limit", 32 * 1024);

  public Pattern regEx;
  public int regExLimit;
  public List<String> namedFields;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    RegExExtractor.logger.trace("config {}: {}", prefix, config);
    final Throwable e = pRegEx.getLastError();
    if (e != null) {
      RegExExtractor.logger.warn("REGEX error: ", e);
    }
    regEx = pRegEx.get();
    regExLimit = pRegExLimit.get();
    if (pRegExExtractNamed.get() && (regEx != null)) {
      namedFields = LibRegEx.getNamedGroup(regEx.pattern());
    }
    else {
      namedFields = null;
    }
  }

  public int getPosition(final String name) {
    if ((namedFields == null) || (name == null)) { return -1; }
    for (int i = 0; i < namedFields.size(); i++) {
      if (name.equals(namedFields.get(i))) { return i; }
    }
    return -1;
  }

  @Override
  public List<String> getNames() {
    return namedFields;
  }

  @Override
  public List<String> getValues(final String value) {
    if ((regEx == null) || (value == null)) { return null; }
    RegExExtractor.logger.trace("Pattern: {} -> body: {}", regEx.pattern(), value);
    List<String> result = null;
    final String match = LibStr.limit(value, regExLimit);
    boolean success = false;
    Matcher matcher = null;
    try {
      matcher = regEx.matcher(match);
      success = matcher.find();
    }
    catch (final StackOverflowError err) {
      RegExExtractor.logger.warn("Pattern too complex: {}", regEx.pattern());
    }
    if (success) {
      if (namedFields != null) {
        final int size = namedFields.size();
        result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          result.add(matcher.group(namedFields.get(i)));
        }
      }
      else {
        final int size = matcher.groupCount() + 1;
        result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          result.add(matcher.group(i));
        }
      }
    }
    return result;
  }

}
