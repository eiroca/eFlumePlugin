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
package net.eiroca.sysadm.flume.plugin;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.ListParameter;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.core.Helper;
import net.eiroca.library.core.LibStr;
import net.eiroca.library.data.PairEntry;
import net.eiroca.library.data.Tags;
import net.eiroca.library.system.Logs;
import net.eiroca.sysadm.flume.api.IExtractor;
import net.eiroca.sysadm.flume.core.actions.Actions;
import net.eiroca.sysadm.flume.core.util.EventSorter;
import net.eiroca.sysadm.flume.core.util.FlumeHelper;
import net.eiroca.sysadm.flume.core.util.LicenseCheck;
import net.eiroca.sysadm.flume.core.util.MacroExpander;
import net.eiroca.sysadm.flume.type.action.ActionExtractor;
import net.eiroca.sysadm.flume.util.interceptors.UltimateConfig;

public class UltimateInterceptor implements Interceptor {

  transient private static final Logger logger = Logs.getLogger();

  // bulk_size * BULK_TIME_LIMIT ms limit in bulk processing
  private static final int BULK_TIME_LIMIT = 5;
  // ms limit in event processing
  private static final int EVENT_TIME_LIMIT = 1000;

  UltimateConfig defaultConfig;
  String[] ruleFormat;
  Comparator<Event> sorter;

  private static final HashMap<String, PairEntry<Date, UltimateConfig>> configCache = new HashMap<>();

  public UltimateInterceptor(final String[] ruleFormat, final String sortHeader, final UltimateConfig defaultConfig) {
    LicenseCheck.runCheck();
    this.defaultConfig = defaultConfig;
    this.ruleFormat = ruleFormat;
    sorter = (sortHeader != null) ? new EventSorter(sortHeader) : null;
    UltimateInterceptor.logger.debug("Default config: {}", this.defaultConfig);
  }

  @Override
  public void initialize() {
    UltimateInterceptor.logger.debug("Initialize {}...", this);
  }

  public UltimateConfig getConfig(final String[] ruleFormat, final Map<String, String> headers) {
    UltimateConfig result = null;
    if (ruleFormat != null) {
      String ruleName = null;
      for (final String rule : ruleFormat) {
        ruleName = MacroExpander.expand(rule, headers, null);
        PairEntry<Date, UltimateConfig> entry;
        // synchronized (UltimateInterceptor.configCache) {
        entry = UltimateInterceptor.configCache.get(ruleName);
        if (entry == null) {
          result = UltimateConfig.readRule(ruleName);
          entry = new PairEntry<>(new Date(), result);
          UltimateInterceptor.configCache.put(ruleName, entry);
        }
        // }
        result = (entry != null) ? entry.getRight() : null;
        if (result != null) {
          break;
        }
      }
      UltimateInterceptor.logger.trace("getConfig({})", ruleName);
    }
    if (result == null) {
      result = defaultConfig;
    }
    return result;
  }

  @Override
  public Event intercept(final Event event) {
    boolean isSuccess = false;
    final long now = System.currentTimeMillis();
    UltimateInterceptor.logger.trace("Intercept {}...", this);
    UltimateConfig config = null;
    try {
      UltimateInterceptor.logger.trace("Intercept Event: {}", event);
      final Map<String, String> headers = event.getHeaders();
      config = getConfig(ruleFormat, headers);
      UltimateInterceptor.logger.trace("Intercept Config: {}", config);
      byte[] data = event.getBody();
      final int oldSize = Helper.size(data);
      try {
        data = UltimateInterceptor.trim(config, data);
      }
      catch (final Exception e) {
        UltimateInterceptor.logger.warn("Ignored error during trim() ", e);
      }
      final int newSize = Helper.size(data);
      boolean changed = newSize != oldSize;
      final String originalBody = LibStr.getMessage(data, config.encoding, FlumeHelper.BODY_ERROR_MESSAGE);
      if (config.filter != null) {
        if (!config.filter.accept(headers, originalBody)) {
          UltimateInterceptor.logger.debug("event skipped: {}", event);
          return null;
        }
      }
      String body = UltimateInterceptor.replace(originalBody, config.standardReplacements, config.replacements);
      changed = changed | (!body.equals(originalBody));
      Actions.execute(config.headers, headers, body);
      Tags fields = null;
      String newBody = null;
      String newEncoding = null;
      if (config.extractors.size() > 0) {
        for (final IExtractor extractor : config.extractors) {
          UltimateInterceptor.logger.trace("Checking: {}", extractor);
          try {
            fields = ActionExtractor.extractFields("%()", extractor, config.extractorsFields, headers, body);
          }
          catch (final Exception e) {
            if (!config.silentError) { throw e; }
            UltimateInterceptor.logger.debug("Ignoring error", e);
          }
          isSuccess = fields != null;
          if (isSuccess) {
            break;
          }
        }
      }
      else {
        isSuccess = true;
      }
      if (isSuccess) {
        if (config.successOutput != null) {
          newBody = MacroExpander.expand(config.successOutput, headers, body, fields.map());
        }
        newEncoding = config.successEncoding;
        Actions.execute(config.successHeaders, headers, newBody != null ? newBody : body);
      }
      else {
        UltimateInterceptor.logger.debug("failed event: {}", originalBody);
        if (config.failedOutput != null) {
          newBody = MacroExpander.expand(config.failedOutput, headers, body);
        }
        newEncoding = config.failedEncoding;
        Actions.execute(config.failedHeaders, headers, newBody != null ? newBody : body);
      }
      if (newBody != null) {
        body = newBody;
        changed = true;
      }
      byte[] newData = data;
      final boolean needRencoding = newEncoding != null ? !newEncoding.equalsIgnoreCase(config.encoding) : false;
      if (changed || needRencoding) {
        if (needRencoding) {
          newData = LibStr.convertCharSet(body, newEncoding);
          if (newData != null) {
            changed = true;
          }
          else {
            newData = body.getBytes();
            UltimateInterceptor.logger.warn("Unable to convert charset {} -> {}", config.encoding, newEncoding);
          }
        }
        else {
          newData = body.getBytes();
        }
      }
      if (config.bodyLimit >= 0) {
        if (newData.length > config.bodyLimit) {
          UltimateInterceptor.logger.debug("Body size limited {} -> {}", newData.length, config.bodyLimit);
          changed = true;
          final byte[] tempData = new byte[config.bodyLimit];
          if (config.bodyLimit > 0) {
            System.arraycopy(newData, 0, tempData, 0, config.bodyLimit);
          }
          newData = tempData;
        }
      }
      if (changed) {
        event.setBody(newData);
      }
    }
    catch (final Exception e) {
      UltimateInterceptor.logger.error("Interceptor unexpexted error: ", e);
    }
    final long elapsed = System.currentTimeMillis() - now;
    UltimateInterceptor.logger.debug("Success: {} event: {}", isSuccess, event);
    if (elapsed > UltimateInterceptor.EVENT_TIME_LIMIT) {
      String confName = (config != null) ? config.rule : "";
      UltimateInterceptor.logger.info("SLOW processing {} ms event. Rule: {}", elapsed, confName);
      UltimateInterceptor.logger.debug("SLOW processing body: {}", event);
    }
    return event;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    if (events == null) { return events; }
    long elapsed = System.currentTimeMillis();
    UltimateInterceptor.logger.debug("Interception {} event(s)", events.size(), this);
    int i = 0;
    final int size = events.size();
    if ((size > 1) && (sorter != null)) {
      final long now = System.currentTimeMillis();
      Collections.sort(events, sorter);
      UltimateInterceptor.logger.trace("Sorted {} events in {}ms", size, (System.currentTimeMillis() - now));
    }
    while (i < events.size()) {
      final Event e = events.get(i);
      final Event newEvent = intercept(e);
      if (newEvent != null) {
        events.set(i, newEvent);
        i++;
      }
      else {
        events.remove(i);
      }
    }
    elapsed = (System.currentTimeMillis() - elapsed);
    UltimateInterceptor.logger.debug("Ultimate kept: {} elapsed: {} ms", events.size(), elapsed);
    if ((size >= 100) && (elapsed > (size * UltimateInterceptor.BULK_TIME_LIMIT))) {
      final Event e = events.size() > 0 ? events.get(0) : null;
      UltimateInterceptor.logger.info("SLOW bulk {} ms 1st event: {} ", elapsed, e);
    }
    UltimateInterceptor.logger.debug("Ultimate kept: {} elapsed: {} ms", events.size(), elapsed);
    return events;
  }

  @Override
  public void close() {
    UltimateInterceptor.logger.debug("Close {}...", this);
  }

  public static String replace(String body, final boolean standardReplacements, final List<PairEntry<String, String>> replacements) {
    if ((body != null)) {
      if (standardReplacements) {
        final StringBuffer newBody = new StringBuffer(body.length());
        char lastChar = (char)0;
        boolean changed = false;
        for (int i = 0; i < body.length(); i++) {
          char ch = body.charAt(i);
          switch (ch) {
            case '\t':
              ch = ' ';
              changed = true;
              break;
            case '\r':
            case '\n':
              changed = true;
              ch = '\t';
              break;
          }
          if (ch != '\t') {
            newBody.append(ch);
          }
          else {
            if (lastChar != '\t') {
              newBody.append(ch);
            }
            else {
              changed = true;
            }
          }
          lastChar = ch;
        }
        if (changed) {
          body = newBody.toString();
        }
      }
      if ((replacements != null) && (replacements.size() > 0)) {
        for (final PairEntry<String, String> replacement : replacements) {
          body = body.replace(replacement.getLeft(), replacement.getRight());
        }
      }
    }
    return body;
  }

  public static byte[] trim(final UltimateConfig config, byte[] body) {
    if ((Helper.isEmptyOrNull(config.lTrim) && Helper.isEmptyOrNull(config.rTrim)) || (body == null)) { return body; }
    final int size = body.length;
    int start = 0;
    int end = size - 1;
    if (!Helper.isEmptyOrNull(config.lTrim)) {
      for (start = 0; start < size; start++) {
        final byte b = body[start];
        boolean found = false;
        for (final byte element : config.lTrim) {
          if (b == element) {
            found = true;
            break;
          }
        }
        if (!found) {
          break;
        }
      }
    }
    if (!Helper.isEmptyOrNull(config.rTrim)) {
      for (end = size - 1; end >= start; end--) {
        final byte b = body[end];
        boolean found = false;
        for (final byte element : config.rTrim) {
          if (b == element) {
            found = true;
            break;
          }
        }
        if (!found) {
          break;
        }
      }
    }
    if ((start != 0) || (end != (size - 1))) {
      byte[] newbody;
      if ((start == size) || (end == 0)) {
        newbody = null;
        UltimateInterceptor.logger.trace("body TRIM {} -> null", body.length);
      }
      else {
        newbody = new byte[(end - start) + 1];
        System.arraycopy(body, start, newbody, 0, newbody.length);
      }
      body = newbody;
      UltimateInterceptor.logger.trace("body TRIM {} -> {}", body != null ? body.length : -1, newbody != null ? newbody.length : -1);
    }
    return body;
  }

  public static class Builder implements Interceptor.Builder {

    final private transient Parameters params = new Parameters();
    final private transient ListParameter pRule = new ListParameter(params, "rule", null);
    final private transient StringParameter pSortHeader = new StringParameter(params, "sort-header", null);

    UltimateConfig config;
    String[] ruleFormat;
    String sortHeader;

    @Override
    public void configure(final Context context) {
      FlumeHelper.laodConfig(params, context);
      ruleFormat = pRule.get();
      sortHeader = pSortHeader.get();
      config = new UltimateConfig(getClass().getName(), context.getParameters());
    }

    @Override
    public Interceptor build() {
      return new UltimateInterceptor(ruleFormat, sortHeader, config);
    }

  }

}
