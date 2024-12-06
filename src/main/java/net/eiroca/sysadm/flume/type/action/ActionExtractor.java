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
package net.eiroca.sysadm.flume.type.action;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IConverterResult;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.actions.Action;
import net.eiroca.sysadm.flume.core.extractors.Extractors;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.type.extractor.util.FieldConfig;

public class ActionExtractor extends Action {

  private static final String NONAME = "?";

  transient private static final Logger logger = Logs.getLogger();

  private static final String CTX_EXTRACTORFIELD_PREFIX = "field.";

  final private transient StringParameter pExtractorType = new StringParameter(params, "parser", Extractors.registry.defaultName());
  final private transient ListParameter pExtractorFields = new ListParameter(params, "fields", null);

  protected IExtractor extractor;
  protected Map<String, FieldConfig> extractorsFields = new HashMap<>();

  @Override
  public void configure(final ImmutableMap<String, String> config, final String prefix) {
    super.configure(config, prefix);
    final String type = pExtractorType.get();
    extractor = Extractors.build(type, config, LibStr.concatenate(prefix, ".", type, "."));
    extractorsFields = ActionExtractor.buildExtractorFields(extractor, pExtractorFields.get(), config, LibStr.concatenate(prefix, ".") + ActionExtractor.CTX_EXTRACTORFIELD_PREFIX);
    ActionExtractor.logger.debug("{} config: {}", getName(), this);
  }

  @Override
  public void run(final Map<String, String> headers, final String body) {
    if (extractor != null) {
      ActionExtractor.logger.trace("Processing {}", getName());
      ActionExtractor.extractFields(extractor, extractorsFields, headers, body);
    }
  }

  public static Tags extractFields(final IExtractor extractor, final Map<String, FieldConfig> extractorsFields, final Map<String, String> headers, final String body) {
    if ((extractor == null) || (body == null)) { return null; }
    ActionExtractor.logger.trace("Extrator: {}", extractor);
    ActionExtractor.logger.trace("Body: {}", body);
    final Tags fields = extractor.getTags(body);
    if (fields != null) {
      // Copy value for default fields
      final Iterator<Entry<String, Object>> x = fields.namedIterator();
      while (x.hasNext()) {
        final Entry<String, Object> e = x.next();
        String name = e.getKey();
        if (!extractorsFields.containsKey(name)) {
          Object extracted = e.getValue();
          if (extracted != null) {
            ActionExtractor.logger.trace(LibStr.concatenate(name, " = ", extracted));
            headers.put(name, String.valueOf(extracted));
          }
        }
      }
      // Extra fields
      for (final Entry<String, FieldConfig> fieldEntry : extractorsFields.entrySet()) {
        final FieldConfig fieldConfig = fieldEntry.getValue();
        if (fieldConfig.name.equals(ActionExtractor.NONAME)) {
          continue;
        }
        String extracted = null;
        String val = fields.getValues(fieldConfig.source, fieldConfig.sourceSep);
        if (val != null) {
          final IConverterResult<?> result = fieldConfig.converter.convert(val);
          if (result.isValid()) {
            Object res = null;
            try {
              res = result.getValue();
            }
            catch (final Exception err) {
              Logs.ignore(err);
            }
            extracted = (res != null) ? String.valueOf(res) : "";
          }
          if (!result.isValid() || (result.getError() != null)) {
            ActionExtractor.logger.info("{} invalid conversion of {}", fieldConfig.name, val);
            ActionExtractor.logger.debug("Invalid conversion", result.getError());
          }
        }
        String value = extracted;
        if (LibStr.isNotEmptyOrNull(extracted)) {
          value = (fieldConfig.expandMacro) ? MacroExpander.expand(extracted, headers) : extracted;
          headers.put(fieldConfig.name, value);
        }
        ActionExtractor.logger.trace(LibStr.concatenate("(", fieldConfig.converter.getName(), ") ", fieldConfig.name, " = ", val, " -> ", value));
      }
    }
    return fields;

  }

  public static Map<String, FieldConfig> buildExtractorFields(final List<IExtractor> extractors, final String[] extractorFieldNames, final ImmutableMap<String, String> config, final String prefix) {
    ActionExtractor.logger.debug("buildExtractorFields: {} ", extractors);
    final Map<String, FieldConfig> extractorsFields = new HashMap<>();
    ActionExtractor.addExtractorFields(extractorsFields, extractorFieldNames, config, prefix);
    return extractorsFields;
  }

  private static Map<String, FieldConfig> buildExtractorFields(final IExtractor extractor, final String[] extractorFieldNames, final ImmutableMap<String, String> config, final String prefix) {
    final Map<String, FieldConfig> extractorsFields = new TreeMap<>();
    ActionExtractor.addExtractorFields(extractorsFields, extractorFieldNames, config, prefix);
    return extractorsFields;
  }

  private static void addExtractorFields(final Map<String, FieldConfig> extractorsFields, final String[] extractorFieldNames, final ImmutableMap<String, String> config, final String prefix) {
    ActionExtractor.logger.trace("{} extractor extra fields", (extractorFieldNames != null) ? extractorFieldNames.length : 0);
    if (extractorFieldNames != null) {
      for (final String name : extractorFieldNames) {
        final FieldConfig res = new FieldConfig(config, prefix, name);
        extractorsFields.put(name, res);
      }
    }
  }

}
