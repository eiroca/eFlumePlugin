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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.core.filters.Filter;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

public class FilterFieldSampler extends Filter {

  private static final Logger logger = Logs.getLogger();

  protected final StringParameter pSampleSource = new StringParameter(params, "sample-source", "%{priority}");
  protected final IntegerParameter pSampleSeed = new IntegerParameter(params, "sample-seed", 0);
  protected final IntegerParameter pSampleDefaultRate = new IntegerParameter(params, "sample-default-rate", 100);
  protected final StringParameter pSampleRateMapping = new StringParameter(params, "sample-rate-mapping", "T=25,I=50,W=100,E=100,F=100,all=100,off=0");

  private Random generator = new Random();
  private String sampleSource;
  private int sampleDefaultRate;
  private Map<String, Integer> rate = new HashMap<>();

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    FilterFieldSampler.logger.debug("FilterFieldSampler Context: {} {}", prefix, config);
    final int seed = pSampleSeed.get();
    if (seed == 0) {
      generator = new Random();
    }
    else {
      generator = new Random(seed);
    }
    sampleSource = pSampleSource.get();
    sampleDefaultRate = pSampleDefaultRate.get();
    final String rateStr = pSampleRateMapping.get();
    try {
      rate = LibStr.parseMapping(rateStr);
    }
    catch (final Exception e) {
      FilterFieldSampler.logger.error("Invalid priority mapping string {}", rateStr, e);
    }
  }

  @Override
  public boolean accept(final Map<String, String> headers, final String body) {
    boolean result = true;
    int r = sampleDefaultRate;
    String _fieldVal = null;
    if (sampleSource != null) {
      _fieldVal = MacroExpander.expand(sampleSource, headers, body);
      if (LibStr.isNotEmptyOrNull(_fieldVal)) {
        final Integer _rate = rate.get(_fieldVal.toLowerCase());
        if (_rate != null) {
          r = _rate;
        }
      }
    }
    if (r <= 0) {
      result = false;
    }
    else if (r >= 100) {
      result = true;
    }
    else {
      final int dice = generator.nextInt(100);
      result = dice < r;
    }
    return result;
  }

}
