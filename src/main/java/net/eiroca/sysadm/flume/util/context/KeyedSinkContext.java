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
package net.eiroca.sysadm.flume.util.context;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flume.Event;
import net.eiroca.sysadm.flume.core.util.BinaryEventSink;
import net.eiroca.sysadm.flume.core.util.GenericSinkContext;

public class KeyedSinkContext<KEY> extends GenericSinkContext<BinaryEventSink<?>> {

  private static final int BUFFER_STARTSIZE = 8192;

  private final Map<KEY, BufferedSinkContext> subContexts = new HashMap<>();

  public KeyedSinkContext(final BinaryEventSink<?> owner) {
    super(owner);
  }

  public void append(final KEY server, final Event event) throws IOException {
    final BufferedSinkContext subContext = getSubContext(server);
    subContext.append(event);
  }

  private synchronized BufferedSinkContext getSubContext(final KEY server) {
    BufferedSinkContext result = subContexts.get(server);
    if (result == null) {
      result = new BufferedSinkContext(owner, KeyedSinkContext.BUFFER_STARTSIZE);
      subContexts.put(server, result);
    }
    return result;
  }

  public Map<KEY, BufferedSinkContext> getSubContexts() {
    return subContexts;
  }

}
