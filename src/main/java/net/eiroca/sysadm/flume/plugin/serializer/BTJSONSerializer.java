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
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import net.eiroca.library.parameter.CharParameter;
import net.eiroca.library.parameter.Parameters;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.Flume;
import net.eiroca.sysadm.flume.type.eventdecoder.ext.DTJSONDecoder;

public class BTJSONSerializer implements EventSerializer, Configurable {

  private final static Logger log = LoggerFactory.getLogger(BTJSONSerializer.class);

  final Parameters params = new Parameters();
  final StringParameter pEncoding = new StringParameter(params, "encoding", "UTF8");
  final CharParameter pLineDelimiter = new CharParameter(params, "line-delimiter", '\n');

  private final OutputStream out;
  private OutputStreamWriter outWriter;

  private String charset;
  private char lineDelimiter;

  private final DTJSONDecoder decoder;
  private final Gson gson;

  public BTJSONSerializer(final OutputStream out) {
    this.out = out;
    final GsonBuilder g = new GsonBuilder();
    gson = g.create();
    decoder = new DTJSONDecoder();
  }

  @Override
  public void configure(final Context context) {
    Flume.laodConfig(params, context);
    charset = pEncoding.get();
    lineDelimiter = pLineDelimiter.get();
    decoder.configure(context.getParameters(), null);
    try {
      outWriter = new OutputStreamWriter(out, charset);
      if (BTJSONSerializer.log.isInfoEnabled()) {
        final StringBuilder sb = new StringBuilder(80);
        sb.append(this.getClass().getSimpleName()).append(": Configured serializer with encoding: '").append(charset).append('\'');
        BTJSONSerializer.log.info(sb.toString());
      }
    }
    catch (final UnsupportedEncodingException uee) {
      final StringBuilder sb = new StringBuilder(80);
      sb.append(this.getClass().getSimpleName()).append(": Unsupported encoding, disabling serializer: '").append(charset).append('\'');
      BTJSONSerializer.log.error(sb.toString(), uee);
    }
  }

  @Override
  public void afterCreate() throws IOException {
  }

  @Override
  public void afterReopen() throws IOException {
  }

  @Override
  public void beforeClose() throws IOException {
  }

  @Override
  public void flush() throws IOException {
    outWriter.flush();
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  void addProperty(final JsonObject jsonElement, final String name, final Object value) {
    if (value != null) {
      jsonElement.addProperty(name, String.valueOf(value));
    }
  }

  @Override
  public void write(final Event event) throws IOException {
    if (outWriter != null) {
      final List<JsonObject> rows = decoder.decode(event);
      for (final JsonObject row : rows) {
        outWriter.write(gson.toJson(row));
        outWriter.write(lineDelimiter);
      }
    }
  }
}
