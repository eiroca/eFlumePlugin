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
package net.eiroca.sysadm.flume.util.tracker;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import net.eiroca.library.config.Parameters;
import net.eiroca.library.config.parameter.StringParameter;
import net.eiroca.library.system.Logs;

public class PrincipalConfig {

  transient private static final Logger logger = Logs.getLogger();

  final transient private Parameters params = new Parameters();

  final transient private StringParameter pDomain = new StringParameter(params, "domain", "");
  final transient private StringParameter pUsername = new StringParameter(params, "username");
  final transient private StringParameter pPassword = new StringParameter(params, "password");

  public String domain;
  public String username;
  public transient String password;

  public PrincipalConfig(final ImmutableMap<String, String> config, final String prefix) {
    configure(config, prefix.endsWith(".") ? prefix : prefix + ".");
  }

  public void configure(final ImmutableMap<String, String> config, final String basePrefix) {
    PrincipalConfig.logger.trace("PrincipalConfig prefix: " + basePrefix);
    params.loadConfig(config, basePrefix);
    domain = pDomain.get();
    username = pUsername.get();
    password = pPassword.get();
    if (password != null) {
      password = new String(Base64.decodeBase64(password));
    }
  }

  @Override
  public String toString() {
    return new Gson().toJson(this).toString();
  }

}
