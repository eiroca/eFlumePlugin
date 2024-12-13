/**
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
package net.eiroca.sysadm.flume.core.converters;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.sysadm.flume.api.IConverter;
import net.eiroca.sysadm.flume.api.IConverterResult;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

abstract public class Converter<T> extends ConfigurableObject implements IConverter<T> {

  final private transient BooleanParameter pSkipNull = new BooleanParameter(params, "skip-null", false);

  protected boolean allowNull;

  abstract protected T doConvert(String value);

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    allowNull = !pSkipNull.get();
  }

  @Override
  public IConverterResult<T> convert(final String value) {
    final ConverterResult<T> result = new ConverterResult<>();
    if (allowNull || (value != null)) {
      try {
        result.value = doConvert(value);
        result.valid = true;
        result.error = null;
      }
      catch (final Exception e) {
        result.value = null;
        result.valid = false;
        result.error = e;
      }
    }
    return result;
  }

}
