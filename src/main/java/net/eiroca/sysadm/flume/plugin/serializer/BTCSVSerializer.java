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
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BtOccurrence;
import com.dynatrace.diagnostics.core.realtime.export.BtExport.BusinessTransaction;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.Flume;
import net.eiroca.sysadm.flume.type.eventdecoder.ext.DTCSVDecoder;

class BTCSVSerializer implements EventSerializer, Configurable {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  final Parameters params = new Parameters();
  final StringParameter pEncoding = new StringParameter(params, "encoding", "UTF8");

  private String charset;

  private final OutputStream out;
  private OutputStreamWriter outWriter;

  private final DTCSVDecoder decoder;

  public BTCSVSerializer(final OutputStream out) {
    this.out = out;
    decoder = new DTCSVDecoder();
  }

  @Override
  public void configure(final Context context) {
    Flume.laodConfig(params, context);
    decoder.configure(context.getParameters(), null);
    charset = pEncoding.get();
    try {
      outWriter = new OutputStreamWriter(out, charset);
      if (log.isInfoEnabled()) {
        final StringBuilder sb = new StringBuilder(80);
        sb.append(this.getClass().getSimpleName()).append(": Configured serializer with encoding: '").append(charset).append('\'');
        log.info(sb.toString());
      }
    }
    catch (final UnsupportedEncodingException uee) {
      final StringBuilder sb = new StringBuilder(80);
      sb.append(this.getClass().getSimpleName()).append(": Unsupported encoding, disabling serializer: '").append(charset).append('\'');
      log.error(sb.toString(), uee);
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
    outWriter = null;
  }

  @Override
  public void flush() throws IOException {
    if (outWriter != null) {
      outWriter.flush();
    }
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  /**
   * Writes the given {@link Event} to the {@link OutputStream}, if its type matches the type of
   * this Serializer. One line is written per {@link BtOccurrence} included in the event. To ensure
   * usability by hive strings are escaped using {@link BTCSVSerializer#escape(String)}.
   * @param event - the {@link Event} to be serialized
   * @see BusinessTransaction.Type
   */
  @Override
  public void write(final Event event) throws IOException {
    if (outWriter != null) {
      final List<String> rows = decoder.decode(event);
      for (final String row : rows) {
        outWriter.write(row);
      }
    }
  }

}
