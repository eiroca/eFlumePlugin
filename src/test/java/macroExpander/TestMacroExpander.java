
/**
 * Copyright (C) 1999-2020 Enrico Croce - AGPL >= 3.0
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
package macroExpander;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import net.eiroca.sysadm.flume.core.util.MacroExpander;

public class TestMacroExpander {

  @Test
  public void macroExpand1() {
    final Map<String, String> h = new HashMap<>();
    final String a = "Hello";
    final String b = "world";
    final String body = a + " " + b + "!";
    h.put("a", a);
    h.put("b", b);
    h.put("ab", body);
    String r;
    r = MacroExpander.expand(body, h, body);
    Assert.assertEquals(r, body);
    r = MacroExpander.expand("%()", h, body);
    Assert.assertEquals(r, body);
    r = MacroExpander.expand("%{ab}", h, body);
    Assert.assertEquals(r, body);
    r = MacroExpander.expand("%{a} %{b}!", h, body);
    Assert.assertEquals(r, body);
    r = MacroExpander.expand(" %() ", h, body);
    Assert.assertEquals(r, " " + body + " ");
  }

  public void macroExpand2() {
    final Map<String, String> h = new HashMap<>();
    final String a = "Hello";
    final String b = "world";
    final String body = a + " " + b + "!";
    h.put("a", a);
    h.put("b", b);
    h.put("c", body);
    String r;
    long t = 0;
    for (int j = 0; j < 10; j++) {
      final long now = System.currentTimeMillis();
      for (int i = 0; i < 20_000_000; i++) {
        h.put("i", "" + i);
        r = MacroExpander.expand(body, h, body);
        Assert.assertEquals(r, body);
        r = MacroExpander.expand("%()", h, body);
        Assert.assertEquals(r, body);
        r = MacroExpander.expand("%{c}", h, body);
        Assert.assertEquals(r, body);
        r = MacroExpander.expand("%{a} %{b}!", h, body);
        Assert.assertEquals(r, body);
      }
      final long e = (System.currentTimeMillis() - now);
      t += e;
      System.out.println("Elapsed " + j + ": " + e);
    }
    System.out.println("total: " + t);
  }

}
