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

public class ExtractedName {

  StringBuilder name = new StringBuilder();
  boolean complete = false;
  int newPos;

  static public ExtractedName extract(final String source, final int start, final char end) {
    final ExtractedName r = new ExtractedName();
    int i;
    for (i = start; i < source.length(); i++) {
      final char ch = source.charAt(i);
      if (ch == end) {
        r.complete = true;
        break;
      }
      r.name.append(ch);
    }
    r.newPos = i + 1;
    return r;
  }
}
