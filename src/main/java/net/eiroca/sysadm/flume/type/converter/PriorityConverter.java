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
package net.eiroca.sysadm.flume.type.converter;

import com.google.common.collect.ImmutableMap;

/**
 * Converter that simply returns the passed in value
 */
public class PriorityConverter extends LookupConverter {

  private static final String PRIORITY_MAPPING = "off=off,none=off,trace=T,debug=T,finer=T,fine=T,notice=I,notification=I,info=I,information=I,warn=W,warning=W,error=E,severe=E,fatal=F,critical=F,panic=F,all=all";

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    if (mapping == null) {
      mapping = parseMapping(PriorityConverter.PRIORITY_MAPPING);
    }
  }

}
