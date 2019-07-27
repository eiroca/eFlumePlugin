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
package net.eiroca.sysadm.flume.plugin;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import net.eiroca.library.core.Helper;

/**
 * Extracts a single field from a JSON event body. The original event body is discarded, and
 * replaced with the extracted value.
 *
 * Events with invalid JSON formatting, or where the extracted value is not a simple string value
 * field, are discarded.
 */
public class JsonFieldExtractorInterceptor implements Interceptor {

  private static final Logger logger = Logger.getLogger(JsonFieldExtractorInterceptor.class);

  private final String propertyName;
  private final JsonFactory jsonFactory = new JsonFactory();

  private JsonFieldExtractorInterceptor(final String propertyName) {
    this.propertyName = propertyName;
  }

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(final Event event) {
    JsonParser parser = null;
    try {
      parser = jsonFactory.createJsonParser(event.getBody());
      // Read past the top level START_OBJECT token
      parser.nextToken();
      if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
        parser.nextToken();
      }
      JsonToken currentToken = parser.getCurrentToken();
      while (currentToken != JsonToken.END_OBJECT) {
        // Match a top level property name
        if (currentToken == JsonToken.FIELD_NAME) {
          final String fieldName = parser.getCurrentName();
          if (propertyName.equals(fieldName)) {
            final JsonToken value = parser.nextToken();
            // Extract the next value as a string
            if (value == JsonToken.VALUE_STRING) {
              event.setBody(parser.getText().getBytes());
              return event;
            }
            else {
              JsonFieldExtractorInterceptor.logger.warn("Discarding event with non-string property value");
              return null;
            }
          }

          // Skip over all array and object values
        }
        else if ((currentToken == JsonToken.START_ARRAY) || (currentToken == JsonToken.START_OBJECT)) {
          parser.skipChildren();
        }
        currentToken = parser.nextToken();
      }
    }
    catch (JsonParseException | UnsupportedEncodingException e) {
      JsonFieldExtractorInterceptor.logger.warn("Discarding event with invalid JSON formatting", e);
    }
    catch (final IOException e) {
      JsonFieldExtractorInterceptor.logger.warn("Problem reading the event contents", e);
    }
    finally {
      Helper.close(parser);
    }
    return null;
  }

  @Override
  public List<Event> intercept(final List<Event> events) {
    final List<Event> result = new ArrayList<>();
    for (final Event e : events) {
      final Event ne = intercept(e);
      if (ne != null) {
        result.add(ne);
      }
    }
    return result;
  }

  @Override
  public void close() {
  }

  /**
   * Used by Flume to create the Interceptor instance.
   */
  public static class Builder implements Interceptor.Builder {

    private String propertyName;

    @Override
    public Interceptor build() {
      return new JsonFieldExtractorInterceptor(propertyName);
    }

    @Override
    public void configure(final Context context) {
      propertyName = context.getString("propertyName");
    }
  }
}
