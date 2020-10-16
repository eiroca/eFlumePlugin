/**
 *
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.core.util.context;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import net.eiroca.sysadm.flume.core.util.GenericSink;

public class SerializerSinkContext extends GenericSinkContext<GenericSink<?>> {

  public EventSerializer serializer;

  public SerializerSinkContext(final GenericSink<?> owner) {
    super(owner);
  }

  public void openSerializer(final OutputStream outputStream) throws IOException {
    if (serializer != null) {
      if (serializer.supportsReopen()) {
        serializer.afterReopen();
      }
      else {
        serializer = null;
      }
    }
    if (serializer == null) {
      serializer = EventSerializerFactory.getInstance(owner.getSerializerType(), owner.getSerializerContext(), outputStream);
      serializer.afterCreate();
    }
  }

  public void closeSerializer() throws IOException {
    if (serializer != null) {
      serializer.beforeClose();
      serializer = null;
    }
  }

}
