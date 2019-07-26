/**
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
package net.eiroca.sysadm.flume.core.util;

import org.apache.flume.Context;
import org.apache.flume.serialization.EventSerializer;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.sysadm.flume.plugin.serializer.FormattedSerializer;

public class BinaryEventSink<T extends GenericSinkContext<?>> extends GenericSink<T> {

  final protected StringParameter pSerializerType = new StringParameter(params, "serializer", FormattedSerializer.Builder.class.getName());

  protected String serializerType;
  protected Context serializerContext;

  @Override
  public void configure(final Context context) {
    super.configure(context);
    serializerType = pSerializerType.get();
    serializerContext = new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));
  }

  public String getSerializerType() {
    return serializerType;
  }

  public Context getSerializerContext() {
    return serializerContext;
  }

}
