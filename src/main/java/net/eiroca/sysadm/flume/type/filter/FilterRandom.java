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
package net.eiroca.sysadm.flume.type.filter;

import java.util.Map;
import java.util.Random;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.DoubleParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.sysadm.flume.core.filters.Filter;

public class FilterRandom extends Filter {

  protected final DoubleParameter pRandomFreq = new DoubleParameter(params, "random-freq", 1.0);
  protected final IntegerParameter pRandomSeed = new IntegerParameter(params, "random-seed", 0);

  private Random generator = new Random();
  private double randomFreq = 1;

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    randomFreq = pRandomFreq.get();
    final int seed = pRandomSeed.get();
    if (seed == 0) {
      generator = new Random();
    }
    else {
      generator = new Random(seed);
    }
  }

  @Override
  public boolean accept(final Map<String, String> headers, final String body) {
    final double dice = generator.nextDouble();
    return (dice <= randomFreq);
  }

}
