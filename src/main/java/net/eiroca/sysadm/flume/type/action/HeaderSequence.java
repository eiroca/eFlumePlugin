/**
 *
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
package net.eiroca.sysadm.flume.type.action;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.LongParameter;
import net.eiroca.sysadm.flume.core.actions.HeaderAction;

public class HeaderSequence extends HeaderAction {

  final private transient LongParameter pInitialValue = new LongParameter(params, "sequence-initial-value", 0);

  public static AtomicLong sequence;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    params.loadConfig(config, prefix);
    HeaderSequence.sequence = new AtomicLong(pInitialValue.get());
  }

  @Override
  public String getValue(final Map<String, String> headers, final String body) {
    final long seqNum = HeaderSequence.sequence.getAndIncrement();
    return String.valueOf(seqNum);
  }

}
