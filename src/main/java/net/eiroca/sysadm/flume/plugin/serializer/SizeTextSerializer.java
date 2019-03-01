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
package net.eiroca.sysadm.flume.plugin.serializer;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;

/**
 * This class simply writes the body of the event to the output stream and appends a newline after
 * each event.
 */
public class SizeTextSerializer extends FormattedSerializer {

  private SizeTextSerializer(final OutputStream out, final Context ctx) {
    super(out, ctx);
  }

  @Override
  public void write(final Event e) throws IOException {
    final byte[] header = new byte[4];
    final byte[] body = format(e);
    int size = body.length + (appendCR ? 1 : 0) + (appendLF ? 1 : 0);
    for (int i = 3; i >= 0; i--) {
      header[i] = (byte)(size & 0xFF);
      size = size >> 8;
    }
    out.write(header);
    out.write(body);
    if (appendCR) {
      out.write(FormattedSerializer.CR);
    }
    if (appendLF) {
      out.write(FormattedSerializer.LF);
    }
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(final Context context, final OutputStream out) {
      final SizeTextSerializer s = new SizeTextSerializer(out, context);
      return s;
    }

  }

}
