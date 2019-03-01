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
package net.eiroca.sysadm.flume.core.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.eiroca.library.core.Helper;

public class MappingCSVData {

  transient protected final static Logger logger = LoggerFactory.getLogger(MappingCSVData.class);

  private final Map<String, Map<String, String>> valuesMap = new HashMap<>();
  private String[] fieldNames;

  public MappingCSVData(final String csvFile, final String csvSeparatorChar) {
    readCSV(csvFile, csvSeparatorChar);
  }

  protected void readCSV(final String csvFile, final String csvSeparatorChar) {
    valuesMap.clear();
    fieldNames = null;
    BufferedReader br = null;
    String line = "";
    try {
      br = new BufferedReader(new FileReader(csvFile));
      String headerLine;
      // Get header line from CSV file
      if ((headerLine = br.readLine()) != null) {
        fieldNames = headerLine.split(csvSeparatorChar);
        // Iterate over the lines of CSV file
        while ((line = br.readLine()) != null) {
          final String[] csvLine = line.split(csvSeparatorChar);
          final String matchValue = csvLine[0];
          final Map<String, String> auxMap = new HashMap<>();
          for (int i = 1; i < fieldNames.length; i++) {
            auxMap.put(fieldNames[i], csvLine[i]);
          }
          valuesMap.put(matchValue, auxMap);
        }
      }
      else {
        throw new IOException("CSV file is empty!");
      }
    }
    catch (final FileNotFoundException e) {
      MappingCSVData.logger.error("File {} not found", csvFile);
    }
    catch (final IOException e) {
      MappingCSVData.logger.error("IOException reading {}", csvFile, e);
    }
    finally {
      Helper.close(br);
    }
  }

  public String getFieldName(final int i) {
    String result = null;
    if ((fieldNames != null) && (i >= 0) && (i < fieldNames.length)) {
      result = fieldNames[i];
    }
    return result;
  }

  public Map<String, String> get(final String key) {
    return valuesMap.get(key);
  }
}
