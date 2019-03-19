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
package net.eiroca.sysadm.flume.type.extractor.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.sysadm.flume.api.IConverter;
import net.eiroca.sysadm.flume.core.Converters;

public class FieldConfig {

  final private transient Parameters paramsField = new Parameters();
  final private transient ListParameter pFieldSource = new ListParameter(paramsField, "source", null);
  final private transient StringParameter pFieldSourceSep = new StringParameter(paramsField, "source-spearator", " ");
  final private transient StringParameter pFieldConverter = new StringParameter(paramsField, "converter", null);
  final private transient BooleanParameter pExpandMacro = new BooleanParameter(paramsField, "expand", false);

  public String name;
  public String[] source;
  public String sourceSep;
  public IConverter<?> converter;
  public boolean expandMacro;

  public FieldConfig(final String name, final String[] source, final String sourceSep, final IConverter<?> converter) {
    init(name, source, sourceSep, converter, false);
  }

  private void init(final String name, final String[] source, final String sourceSep, final IConverter<?> converter, final boolean expandMacro) {
    this.name = name;
    this.source = source;
    this.sourceSep = sourceSep;
    this.converter = converter;
    this.expandMacro = expandMacro;
  }

  public FieldConfig(final ImmutableMap<String, String> config, final String prefix, final String name) {
    final String baseKey = LibStr.concatenate(prefix, name, ".");
    paramsField.loadConfig(config, baseKey);
    String[] source = pFieldSource.get();
    if (source == null) {
      source = new String[] {
          name
      };
    }
    String converterName = pFieldConverter.get();
    if (converterName == null) {
      converterName = Converters.registry.isValid(name) ? name : Converters.registry.defaultName();
    }
    final IConverter<?> converter = Converters.build(converterName, config, baseKey);
    init(name, source, pFieldSourceSep.get(), converter, pExpandMacro.get());
    Preconditions.checkNotNull(converter, "Could not instantiate converter: {}", converterName);
  }

}
