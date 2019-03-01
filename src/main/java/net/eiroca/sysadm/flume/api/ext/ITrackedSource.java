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
package net.eiroca.sysadm.flume.api.ext;

import java.io.IOException;
import java.util.List;
import org.apache.flume.Event;
import org.apache.flume.client.avro.ReliableEventReader;
import net.eiroca.sysadm.flume.api.IConfigurable;
import net.eiroca.sysadm.flume.util.tracker.watcher.WatcherConfig;

public interface ITrackedSource extends IConfigurable, ReliableEventReader {

  public boolean isOpen();

  public boolean isChanged();

  public boolean isChanged(String oldID, long minSize);

  public void open(final long pos) throws IOException;

  public void seek(final long pos) throws IOException;

  public void commit(long pos);

  public void rollback() throws IOException;

  public List<Event> readEvents(final int numEvents, final boolean backoffWithoutNL, final boolean flush) throws IOException;

  public WatcherConfig getConfig();

  public String getID();

  public String getSource();

  public long getCommittedPosition();

  public long getOpenDate();
}
