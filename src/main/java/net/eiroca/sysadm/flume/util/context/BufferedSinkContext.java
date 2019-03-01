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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.flume.Event;
import net.eiroca.sysadm.flume.core.util.BinaryEventSink;

public class BufferedSinkContext extends SerializerSinkContext {

  int minBufferSize;
  ByteArrayOutputStream buffer;

  public BufferedSinkContext(final BinaryEventSink<?> owner, final int minBufferSize) {
    super(owner);
    this.minBufferSize = minBufferSize;
  }

  public void openSerializer() throws IOException {
    buffer = new ByteArrayOutputStream(minBufferSize);
    openSerializer(buffer);
  }

  public void append(final Event event) throws IOException {
    if (serializer == null) {
      openSerializer();
    }
    serializer.write(event);
  }

  public byte[] getBuffer() {
    final byte[] result = buffer.toByteArray();
    buffer.reset();
    return result;
  }

}
