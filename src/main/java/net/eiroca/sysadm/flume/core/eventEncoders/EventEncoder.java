/**
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
package net.eiroca.sysadm.flume.core.eventEncoders;

import java.util.HashMap;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import net.eiroca.sysadm.flume.api.IEventEncoder;
import net.eiroca.sysadm.flume.api.IEventNotify;
import net.eiroca.sysadm.flume.core.util.ConfigurableObject;

public abstract class EventEncoder<T> extends ConfigurableObject implements IEventEncoder<T> {

  protected IEventNotify callback;

  public Event newEvent() {
    final Event event = new SimpleEvent();
    final HashMap<String, String> headers = new HashMap<>();
    headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
    event.setHeaders(headers);
    return event;
  }

  @Override
  public void setCallBack(final IEventNotify callback) {
    this.callback = callback;
  }

}
