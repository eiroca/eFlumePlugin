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
package net.eiroca.sysadm.flume.util.ultimate;

import java.util.Comparator;
import org.apache.flume.Event;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

final public class EventSorter implements Comparator<Event> {

  String header;

  public EventSorter(final String header) {
    this.header = header;
  }

  @Override
  public int compare(final Event lhs, final Event rhs) {
    // -1 - less than, 1 - greater than, 0 - equal, all inverted for descending
    final String l = MacroExpander.expand(header, lhs.getHeaders());
    final String r = MacroExpander.expand(header, rhs.getHeaders());
    return l.compareTo(r);
  }
}
