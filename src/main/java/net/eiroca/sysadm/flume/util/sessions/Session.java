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
package net.eiroca.sysadm.flume.util.sessions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import net.eiroca.ext.library.gson.JSonUtil;

public class Session {

  public transient Session parent;
  public transient List<Session> childs = new ArrayList<>();
  public UUID id;
  public String key;
  public String validating;
  public long createDate;
  public long lastDate;
  public long expireDate;
  public boolean expired;
  public long TTL = 15 * 60 * 1000;

  public Session(final String key, final long timestamp, final String validating) {
    id = UUID.randomUUID();
    this.key = key;
    this.validating = validating;
    expired = false;
    createDate = timestamp;
    lastDate = createDate;
    expireDate = -1;
  }

  public void setParent(final Session parent) {
    if (this.parent != null) {
      if (this.parent != parent) {
        this.parent.childs.remove(this);
        this.parent = parent;
        parent.childs.add(this);
      }
    }
    else {
      if (parent != null) {
        this.parent = parent;
        parent.childs.add(this);
      }
      else {
      }
    }
  }

  public String ID() {
    return (parent != null) ? parent.ID() : id.toString();
  }

  public String fullID(final String separator) {
    final StringBuffer outID = new StringBuffer();
    fullID(outID, separator);
    return outID.toString();
  }

  public void fullID(final StringBuffer id, final String separator) {
    if (parent != null) {
      parent.fullID(id, separator);
    }
    if (id.length() > 0) {
      id.append(separator);
    }
    id.append(id.toString());
  }

  public void update(final long timestamp) {
    if (parent != null) {
      parent.update(timestamp);
    }
    lastDate = timestamp;
  }

  public synchronized Session validate(final long timestamp, final String validValue) {
    if (!isValid(timestamp)) { return null; }
    Session result = this;
    if (validValue == null) {
      if (validating != null) {
        invalidate(timestamp);
        result = null;
      }
    }
    else {
      if (validating == null) {
        validating = validValue;
      }
      else {
        if (!validating.equals(validValue)) {
          invalidate(timestamp);
          result = null;
        }
      }
    }
    return result;
  }

  public synchronized boolean isValid(final long timestamp) {
    if (!expired) {
      if ((timestamp - lastDate) > TTL) {
        invalidate(timestamp);
      }
    }
    return !expired;
  }

  public synchronized void invalidate(final long timestamp) {
    expired = true;
    expireDate = timestamp;
    if (parent != null) {
      parent.childs.remove(this);
      parent = null;
    }
    for (int i = childs.size() - 1; i >= 0; i--) {
      childs.get(i).invalidate(timestamp);
    }
  }

  @Override
  public String toString() {
    return JSonUtil.toJSON(this);
  }

}
