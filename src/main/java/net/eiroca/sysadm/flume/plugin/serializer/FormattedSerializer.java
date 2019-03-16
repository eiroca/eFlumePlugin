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
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.Flume;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

/**
 * This class simply writes the body of the event to the output stream and appends a newline after
 * each event.
 */
public class FormattedSerializer implements EventSerializer {

  final protected static byte CR = 13;
  final protected static byte LF = 10;

  final private Parameters params = new Parameters();
  final private StringParameter pEncoding = new StringParameter(params, "encoding", "utf-8");
  final private StringParameter pMessageFormat = new StringParameter(params, "format", "%()", false, true);
  final private BooleanParameter pMessageCR = new BooleanParameter(params, "appendCR", true);
  final private BooleanParameter pMessageLF = new BooleanParameter(params, "appendLF", true);

  protected final OutputStream out;

  protected String messageFormat;
  protected boolean appendCR;
  protected boolean appendLF;
  protected String encoding;

  protected FormattedSerializer(final OutputStream out, final Context context) {
    this.out = out;
    Flume.laodConfig(params, context);
    encoding = pEncoding.get();
    messageFormat = pMessageFormat.get();
    appendCR = pMessageCR.get();
    appendLF = pMessageLF.get();
  }

  @Override
  public boolean supportsReopen() {
    return true;
  }

  @Override
  public void afterCreate() {
  }

  @Override
  public void afterReopen() {
  }

  @Override
  public void beforeClose() {
  }

  public byte[] format(final Event e) {
    byte[] result;
    if (messageFormat != null) {
      final String body = Flume.getBody(e, encoding);
      final String _message = MacroExpander.expand(messageFormat, e.getHeaders(), body);
      result = _message.getBytes();
    }
    else {
      result = e.getBody();
    }
    return result;
  }

  @Override
  public void write(final Event e) throws IOException {
    final byte[] body = format(e);
    out.write(body);
    if (appendCR) {
      out.write(FormattedSerializer.CR);
    }
    if (appendLF) {
      out.write(FormattedSerializer.LF);
    }
  }

  @Override
  public void flush() throws IOException {
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(final Context context, final OutputStream out) {
      final EventSerializer s = new FormattedSerializer(out, context);
      return s;
    }

  }

}
