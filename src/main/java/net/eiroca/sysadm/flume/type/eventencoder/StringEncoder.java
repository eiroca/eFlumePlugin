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
package net.eiroca.sysadm.flume.type.eventencoder;

import java.io.UnsupportedEncodingException;
import org.apache.flume.Event;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.EventEncoder;

public class StringEncoder extends EventEncoder<String> {

  final private transient StringParameter pEncoding = new StringParameter(params, "encoding", "UTF-8");

  public String encoding;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    params.laodConfig(config, prefix);
    encoding = pEncoding.get();
  }

  @Override
  public void encode(final String data) {
    if (data == null) { return; }
    final Event event = newEvent();
    try {
      event.setBody(data.getBytes(encoding));
    }
    catch (final UnsupportedEncodingException e) {
    }
    callback.notifyEvent(event);
  }

}
