/**
 *
 * Copyright (C) 1999-2021 Enrico Croce - AGPL >= 3.0
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
package net.eiroca.sysadm.flume.util.tracker;

import net.eiroca.ext.library.gson.GsonUtil;
import net.eiroca.sysadm.flume.api.ext.ITrackedSource;

public class SourceTrack {

  String id;
  String source;
  boolean idle;
  ITrackedSource tracker;
  boolean changed;

  long events;
  long lastCheck;
  long lastPos;

  long previousCheck;
  long previousEvents;
  long previousPos;

  int rotations;

  public SourceTrack(final String id, final String source) {
    this.id = id;
    this.source = source;
    idle = false;
    lastCheck = 0;
    previousCheck = 0;
    events = 0;
    previousEvents = 0;
    rotations = 0;
    changed = true;
  }

  @Override
  public String toString() {
    return GsonUtil.toJSON(this);
  }

  public String me() {
    return String.format("%s [%s]", source, id);
  }

  public void checkPoint(final long date, final long pos) {
    previousCheck = lastCheck;
    previousPos = lastPos;
    lastCheck = date;
    lastPos = pos;
  }

  public void rotate() {
    previousEvents = events;
    previousPos = previousPos - lastPos;
    lastPos = 0;
    rotations++;
  }

  public void addEvents(final int size) {
    events += size;
  }

}
