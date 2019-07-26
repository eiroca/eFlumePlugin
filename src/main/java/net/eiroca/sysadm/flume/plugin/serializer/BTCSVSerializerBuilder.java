/**
 *
 * Copyright (C) 1999-2019 Enrico Croce - AGPL >= 3.0
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

import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.serialization.EventSerializer;

public class BTCSVSerializerBuilder implements EventSerializer.Builder {

  /**
   * Builds a {@link BTCSVSerializer}.
   */
  @Override
  public EventSerializer build(final Context context, final OutputStream outputStream) {
    final BTCSVSerializer serializer = new BTCSVSerializer(outputStream);
    serializer.configure(context);
    return serializer;
  }

}
