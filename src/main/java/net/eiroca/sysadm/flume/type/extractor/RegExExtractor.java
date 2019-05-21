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

import java.util.List;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.regex.ARegEx;
import net.eiroca.library.regex.LibRegEx;
import net.eiroca.library.regex.RegularExpression;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.util.Extractor;

/**
 * Converter that simply returns the passed in value
 */
public class RegExExtractor extends Extractor {

  transient private static final Logger logger = Logs.getLogger();

  final private transient StringParameter pRegEx = new StringParameter(params, "pattern");
  final private transient BooleanParameter pRegExExtractNamed = new BooleanParameter(params, "extract-named", true);
  final private transient IntegerParameter pRegExEngine = new IntegerParameter(params, "regex-engine", 0);
  final private transient IntegerParameter pRegExLimit = new IntegerParameter(params, "size-limit", 32 * 1024);

  public ARegEx regEx;
  public int regExLimit;
  public List<String> namedFields;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    RegExExtractor.logger.trace("config {}: {}", prefix, config);
    final String pattern = pRegEx.get();
    regEx = RegularExpression.build(getName(), pattern, pRegExEngine.get());
    if (regEx == null) {
      RegExExtractor.logger.warn("REGEX error: {}", pattern);
    }
    regEx.setSizeLimit(pRegExLimit.get());
    if (pRegExExtractNamed.get() && (regEx != null)) {
      namedFields = LibRegEx.getNamedGroup(pattern);
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
    RegExExtractor.logger.trace("Pattern: {} -> body: {}", regEx.pattern, value);
    List<String> result;
    if (namedFields != null) {
      result = regEx.extract(namedFields, value);
    }
    else {
      result = regEx.extract(value);
    }
    return result;
  }

}
