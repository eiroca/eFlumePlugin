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
package net.eiroca.sysadm.flume.type.filter;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.parameter.IntegerParameter;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.sysadm.flume.core.util.Filter;
import net.eiroca.sysadm.flume.core.util.PriorityHelper;

public class FilterPriority extends Filter {

  final StringParameter pPrioritySource = new StringParameter(params, "priority-source", null);
  final IntegerParameter pPriorityDefault = new IntegerParameter(params, "priority-default", PriorityHelper.DEFAULT_PRIORITY);
  final StringParameter pPriorityMapping = new StringParameter(params, "priority-mapping", PriorityHelper.DEFAULT_PRIORITY_MAPPING);
  final IntegerParameter pPriorityMinimum = new IntegerParameter(params, "priority-minimum", 0);
  final IntegerParameter pPriorityMaximum = new IntegerParameter(params, "priority-maximum", Integer.MAX_VALUE);

  PriorityHelper priorityHelper = new PriorityHelper();

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    priorityHelper.source = pPrioritySource.get();
    priorityHelper.priorityDefault = pPriorityDefault.get();
    priorityHelper.setPriorityMapping(pPriorityMapping.get());
    priorityHelper.priorityMinimum = pPriorityMinimum.get();
    priorityHelper.priorityMaximum = pPriorityMaximum.get();
  }

  @Override
  public boolean accept(final Map<String, String> headers, final String body) {
    return priorityHelper.isEnabled(headers, body);
  }

}
