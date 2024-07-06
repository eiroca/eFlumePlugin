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
package net.eiroca.sysadm.flume.core.filters;

import com.google.common.collect.ImmutableMap;
import net.eiroca.library.core.Registry;
import net.eiroca.sysadm.flume.api.IEventFilter;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.type.filter.FilterContains;
import net.eiroca.sysadm.flume.type.filter.FilterEndsWith;
import net.eiroca.sysadm.flume.type.filter.FilterFieldSampler;
import net.eiroca.sysadm.flume.type.filter.FilterKeywords;
import net.eiroca.sysadm.flume.type.filter.FilterNull;
import net.eiroca.sysadm.flume.type.filter.FilterPriority;
import net.eiroca.sysadm.flume.type.filter.FilterRandom;
import net.eiroca.sysadm.flume.type.filter.FilterRegEx;
import net.eiroca.sysadm.flume.type.filter.FilterSampler;
import net.eiroca.sysadm.flume.type.filter.FilterStartsWith;

public class Filters extends Registry<String> {

  public static final Registry<String> registry = new Registry<String>();

  static {
    Filters.registry.addEntry(FilterRegEx.class.getName());
    Filters.registry.addEntry("null", FilterNull.class.getName());
    Filters.registry.addEntry("notnull", FilterNull.class.getName());
    Filters.registry.addEntry("body-not-emty", FilterNull.class.getName());
    Filters.registry.addEntry("regex", FilterRegEx.class.getName());
    Filters.registry.addEntry("start", FilterStartsWith.class.getName());
    Filters.registry.addEntry("starts", FilterStartsWith.class.getName());
    Filters.registry.addEntry("startswith", FilterStartsWith.class.getName());
    Filters.registry.addEntry("end", FilterEndsWith.class.getName());
    Filters.registry.addEntry("ends", FilterEndsWith.class.getName());
    Filters.registry.addEntry("endswith", FilterEndsWith.class.getName());
    Filters.registry.addEntry("begin", FilterStartsWith.class.getName());
    Filters.registry.addEntry("begins", FilterStartsWith.class.getName());
    Filters.registry.addEntry("contain", FilterContains.class.getName());
    Filters.registry.addEntry("contains", FilterContains.class.getName());
    Filters.registry.addEntry("is", FilterKeywords.class.getName());
    Filters.registry.addEntry("in", FilterKeywords.class.getName());
    Filters.registry.addEntry("if", FilterKeywords.class.getName());
    Filters.registry.addEntry("keyword", FilterKeywords.class.getName());
    Filters.registry.addEntry("keywords", FilterKeywords.class.getName());
    Filters.registry.addEntry("priority", FilterPriority.class.getName());
    Filters.registry.addEntry("sample", FilterSampler.class.getName());
    Filters.registry.addEntry("sampler", FilterSampler.class.getName());
    Filters.registry.addEntry("random", FilterRandom.class.getName());
    Filters.registry.addEntry("limiter", FilterFieldSampler.class.getName());
    Filters.registry.addEntry("field-sampler", FilterFieldSampler.class.getName());
  }

  public static IEventFilter build(final String type, final ImmutableMap<String, String> config, final String prefix) {
    return (IEventFilter)FlumeHelper.buildIConfigurable(Filters.registry.value(type), config, prefix);
  }

  public static IEventFilter buildFilter(final ImmutableMap<String, String> config, final String prefix, String filterType, final String filterMatch) {
    IEventFilter filter = null;
    if ((filterType == null) && (filterMatch != null)) {
      filterType = "regex";
    }
    if (filterType != null) {
      filter = Filters.build(filterType, config, prefix);
    }
    return filter;
  }

  public static IEventFilter buildFilter(final ImmutableMap<String, String> config, final String prefix, final String filterType) {
    IEventFilter filter = null;
    if (filterType != null) {
      filter = Filters.build(filterType, config, prefix);
    }
    return filter;
  }

}
