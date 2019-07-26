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
package net.eiroca.sysadm.flume.core;

import net.eiroca.sysadm.flume.api.IConverterResult;

public class ConverterResult<T> implements IConverterResult<T> {

  public boolean valid;
  public T value;
  public Exception error;

  @Override
  public boolean isValid() {
    return valid;
  }

  public void setValid(final boolean valid) {
    this.valid = valid;
  }

  public T getResult() {
    return value;
  }

  public void setValue(final T value) {
    this.value = value;
  }

  @Override
  public Exception getError() {
    return error;
  }

  public void setError(final Exception error) {
    this.error = error;
  }

  @Override
  public T getValue() throws Exception {
    if (valid) {
      return value;
    }
    else if (error != null) {
      throw error;
    }
    else {
      return null;
    }
  }
}
