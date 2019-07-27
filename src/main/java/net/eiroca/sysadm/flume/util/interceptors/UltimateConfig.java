/**
 *
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
package net.eiroca.sysadm.flume.util.interceptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.BooleanParameter;
import net.eiroca.library.config.parameter.IntegerParameter;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.data.PairEntry;
import net.eiroca.sysadm.flume.api.IAction;
import net.eiroca.sysadm.flume.api.IEventFilter;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.actions.Actions;
import net.eiroca.sysadm.flume.core.extractors.Extractors;
import net.eiroca.sysadm.flume.core.filters.Filters;
import net.eiroca.sysadm.flume.plugin.UltimateInterceptor;
import net.eiroca.sysadm.flume.type.action.ActionExtractor;
import net.eiroca.sysadm.flume.type.extractor.util.FieldConfig;

public class UltimateConfig {

  public static final Logger logger = LoggerFactory.getLogger(UltimateInterceptor.class);

  private static final String CTX_HEADER_PREFIX = "header.";
  private static final String CTX_BODY_PREFIX = "body.";
  private static final String CTX_FITLER_PREFIX = "filter.";
  private static final String CTX_EXTRACTOR_PREFIX = "extractor.";
  private static final String CTX_SUCCESS_PREFIX = "success.";
  private static final String CTX_FAILED_PREFIX = "failed.";
  private static final String CTX_EXTRACTORFIELD_PREFIX = "extractor.field.";
  private static final String CTX_REPLACEMENT = "replacement.";

  final private transient Parameters params = new Parameters();
  final private transient ListParameter pHeaders = new ListParameter(params, "headers", null);
  final private transient StringParameter pEncoding = new StringParameter(params, "encoding", "UTF-8");
  final private transient BooleanParameter pSilentError = new BooleanParameter(params, "silent-error", true);
  final private transient StringParameter pFilterType = new StringParameter(params, UltimateConfig.CTX_FITLER_PREFIX + "type", null);
  final private transient StringParameter pFilterMatch = new StringParameter(params, UltimateConfig.CTX_FITLER_PREFIX + "match", null);

  final private transient Parameters paramsBody = new Parameters();
  final private transient StringParameter pTrim = new StringParameter(paramsBody, "trim", null, true);
  final private transient StringParameter pLTrim = new StringParameter(paramsBody, "trim-left", null, true);
  final private transient StringParameter pRTrim = new StringParameter(paramsBody, "trim-right", "\n\r\t ", true);
  final private transient BooleanParameter pReplacementsStandard = new BooleanParameter(paramsBody, "standard-replacements", false);
  final private transient ListParameter pReplacementsCustom = new ListParameter(paramsBody, "replacements", null);
  final private transient IntegerParameter pBodyLimit = new IntegerParameter(paramsBody, "limit", -1);

  final private transient Parameters paramsReplacement = new Parameters();
  final private transient StringParameter pReplacementFrom = new StringParameter(paramsReplacement, "from", null, true);
  final private transient StringParameter pReplacementTo = new StringParameter(paramsReplacement, "to", null, true);

  final private transient Parameters paramsExtractor = new Parameters();
  final private transient StringParameter pExtractorType = new StringParameter(paramsExtractor, "type", null);
  final private transient ListParameter pExtractorFields = new ListParameter(paramsExtractor, "fields", null);

  final private transient Parameters paramsSuccess = new Parameters();
  final private transient StringParameter pSuccessOutput = new StringParameter(paramsSuccess, "output", null);
  final private transient ListParameter pSuccessHeaders = new ListParameter(paramsSuccess, "headers", null);
  final private transient StringParameter pSuccessEncoding = new StringParameter(paramsSuccess, "encoding-new", null);

  final private transient Parameters paramsFailed = new Parameters();
  final private transient StringParameter pFailedOutput = new StringParameter(paramsFailed, "output", null);
  final private transient ListParameter pFailedHeaders = new ListParameter(paramsFailed, "headers", null);
  final private transient StringParameter pFailedEncoding = new StringParameter(paramsFailed, "encoding-new", null);

  public String encoding;
  public boolean silentError;

  public List<PairEntry<String, String>> replacements = new ArrayList<>();
  public boolean standardReplacements;

  public List<IAction> headers = new ArrayList<>();

  public byte[] lTrim = null;
  public byte[] rTrim = null;

  public int bodyLimit = -1;

  public IEventFilter filter;

  public List<IExtractor> extractors = new ArrayList<>();
  public Map<String, FieldConfig> extractorsFields = new HashMap<>();

  public String successOutput;
  public String successEncoding;
  public List<IAction> successHeaders = new ArrayList<>();

  public String failedOutput;
  public String failedEncoding;
  public List<IAction> failedHeaders = new ArrayList<>();

  public UltimateConfig(final String name, final ImmutableMap<String, String> config) {
    configure(config);
    UltimateConfig.logger.info("Config for {}: {}", name, this);
  }

  protected void configure(final ImmutableMap<String, String> config) {
    UltimateConfig.logger.trace("Starting config");
    params.loadConfig(config, null);
    encoding = pEncoding.get();
    silentError = pSilentError.get();
    // Headers
    Actions.load(pHeaders.get(), config, UltimateConfig.CTX_HEADER_PREFIX, headers);
    // Body
    paramsBody.loadConfig(config, UltimateConfig.CTX_BODY_PREFIX);
    bodyLimit = pBodyLimit.get();
    final String trim = pTrim.get();
    if (trim != null) {
      lTrim = trim.getBytes();
      rTrim = trim.getBytes();
    }
    else {
      final String lTrimStr = pLTrim.get();
      final String rTrimStr = pRTrim.get();
      lTrim = (lTrimStr != null) ? lTrimStr.getBytes() : null;
      rTrim = (rTrimStr != null) ? rTrimStr.getBytes() : null;
    }
    standardReplacements = pReplacementsStandard.get();
    replacements.clear();
    loadCustomReplacement(config, replacements, pReplacementsCustom.get());
    // Filter
    filter = Filters.buildFilter(config, UltimateConfig.CTX_FITLER_PREFIX, pFilterType.get(), pFilterMatch.get());
    // Transform
    IExtractor extractor;
    extractors.clear();
    paramsExtractor.loadConfig(config, UltimateConfig.CTX_EXTRACTOR_PREFIX);
    String type = pExtractorType.get();
    if ((type == null) && (config.get(UltimateConfig.CTX_EXTRACTOR_PREFIX + "regex.pattern") != null)) {
      type = "regex";
    }
    if (type != null) {
      extractor = Extractors.build(type, config, LibStr.concatenate(UltimateConfig.CTX_EXTRACTOR_PREFIX, type, "."));
      extractors.add(extractor);
    }
    for (int i = 0; i < 10; i++) {
      final String basePrefix = LibStr.concatenate(UltimateConfig.CTX_EXTRACTOR_PREFIX, i, ".");
      type = config.get(LibStr.concatenate(basePrefix, "type"));
      if ((type == null) && (config.get(basePrefix + "regex.pattern") != null)) {
        type = "regex";
      }
      if (type != null) {
        extractor = Extractors.build(type, config, LibStr.concatenate(LibStr.concatenate(basePrefix, type, ".")));
        if (extractor != null) {
          extractors.add(extractor);
        }
      }
    }
    extractorsFields = ActionExtractor.buildExtractorFields(extractors, pExtractorFields.get(), config, UltimateConfig.CTX_EXTRACTORFIELD_PREFIX);
    // Success Actions
    paramsSuccess.loadConfig(config, UltimateConfig.CTX_SUCCESS_PREFIX);
    successOutput = pSuccessOutput.get();
    successEncoding = pSuccessEncoding.get();
    Actions.load(pSuccessHeaders.get(), config, UltimateConfig.CTX_SUCCESS_PREFIX + UltimateConfig.CTX_HEADER_PREFIX, successHeaders);
    // Failed Actions
    paramsFailed.loadConfig(config, UltimateConfig.CTX_FAILED_PREFIX);
    failedOutput = pFailedOutput.get();
    failedEncoding = pFailedEncoding.get();
    Actions.load(pFailedHeaders.get(), config, UltimateConfig.CTX_FAILED_PREFIX + UltimateConfig.CTX_HEADER_PREFIX, failedHeaders);
  }

  private void loadCustomReplacement(final ImmutableMap<String, String> config, final List<PairEntry<String, String>> replacements, final String[] customReplacements) {
    if ((customReplacements == null) || (customReplacements.length == 0)) { return; }
    for (final String replacement : customReplacements) {
      paramsReplacement.loadConfig(config, LibStr.concatenate(UltimateConfig.CTX_REPLACEMENT, replacement));
      final String from = pReplacementFrom.get();
      if (from == null) {
        continue;
      }
      String to = pReplacementTo.get();
      if (to == null) {
        to = "";
      }
      replacements.add(new PairEntry<>(from, to));
    }
  }

  @Override
  public String toString() {
    return new Gson().toJson(this).toString();
  }

}
