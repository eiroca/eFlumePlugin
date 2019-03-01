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

import org.slf4j.Logger;
import net.eiroca.library.license.api.License;
import net.eiroca.library.license.api.LicenseManager;
import net.eiroca.library.system.Logs;

public class LicenseCheck {

  private static final String ME = "eFlumePlugin";

  private static final Logger logger = Logs.getLogger();

  private static License license;

  public static synchronized boolean init() {
    if (LicenseCheck.license == null) {
      LicenseCheck.license = LicenseManager.getInstance().getLicense(LicenseCheck.ME, false);
      return true;
    }
    return false;
  }

  public static boolean isValid() {
    return LicenseManager.isValidLicense(LicenseCheck.license);
  }

  public static String getHolder() {
    return (LicenseCheck.license != null) ? LicenseCheck.license.getHolder() : null;
  }

  public static synchronized void runCheck() {
    final boolean firstTime = LicenseCheck.init();
    if (LicenseCheck.isValid()) {
      if (firstTime) {
        LicenseCheck.logger.info(LicenseCheck.ME + " licenced to " + LicenseCheck.getHolder());
      }
    }
    else {
      throw new RuntimeException("Licence is invalid or expired!");
    }
  }

}
