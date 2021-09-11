/**
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
package net.eiroca.sysadm.flume.type.filter;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.sysadm.flume.core.filters.Filter;

public class FilterSampler extends Filter {

  final IntegerParameter pSampleRate = new IntegerParameter(params, "sample-rate", 1);
  final IntegerParameter pSampleSize = new IntegerParameter(params, "sample-size", 1);

  public int counter = 0;
  public int sampleRate = 1;
  public int sampleSize = 1;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    counter = 0;
    sampleRate = pSampleRate.get();
    sampleSize = pSampleSize.get();
  }

  @Override
  public boolean accept(final Map<String, String> headers, final String body) {
    counter++;
    final boolean accept = counter <= sampleRate;
    if (counter >= sampleSize) {
      counter = 0;
    }
    return accept;
  }

}
