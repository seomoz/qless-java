package com.moz.qless.client;

import java.io.IOException;

import com.google.common.base.Strings;

import org.junit.Assert;
import org.junit.Test;

public class ClientHelperTest {

  @Test
  public void generatesJid() {
      final String jid = ClientHelper.generateJid();
      Assert.assertTrue(jid.matches("\\A[a-f0-9]{32}\\z"));
  }

  @Test
  public void getHostName() throws IOException {
      final String hostName = ClientHelper.getHostName();
      Assert.assertFalse(Strings.isNullOrEmpty(hostName));
  }

  @Test
  public void getPid() throws IOException {
      final String pid = ClientHelper.getPid();
      Assert.assertTrue(!Strings.isNullOrEmpty(pid) && pid.matches("[0-9]+"));
  }
}
