package io.hstream.util;

import static org.junit.jupiter.api.Assertions.*;

import io.hstream.HStreamDBClientException;
import io.hstream.UrlSchema;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class UrlSchemaUtilsTest {

  @Test
  public void testParseServerUrls() {
    // Test valid URL
    String url = "hstream://127.0.0.1:6570,127.0.0.2:6570";
    Pair<UrlSchema, List<String>> result = UrlSchemaUtils.parseServerUrls(url);
    assertEquals(UrlSchema.HSTREAM, result.getLeft());
    assertEquals(2, result.getRight().size());
    assertTrue(result.getRight().contains("127.0.0.1:6570"));
    assertTrue(result.getRight().contains("127.0.0.2:6570"));

    // Test invalid URL - no schema
    String invalidUrl = "127.0.0.1:6570,127.0.0.2:6570";
    assertThrows(HStreamDBClientException.class, () -> UrlSchemaUtils.parseServerUrls(invalidUrl));

    // Test invalid URL - invalid schema
    String invalidUrl2 = "invalid://127.0.0.1:6570,127.0.0.2:6570";
    assertThrows(HStreamDBClientException.class, () -> UrlSchemaUtils.parseServerUrls(invalidUrl2));

    // Test URL with default port
    String urlWithDefaultPort = "hstream://127.0.0.1";
    Pair<UrlSchema, List<String>> result2 = UrlSchemaUtils.parseServerUrls(urlWithDefaultPort);
    assertEquals(UrlSchema.HSTREAM, result2.getLeft());
    assertEquals(1, result2.getRight().size());
    assertEquals("127.0.0.1:6570", result2.getRight().get(0));
  }

  @Test
  public void testNormalizeHost() {
    // Test host with default port
    String hostWithDefaultPort = "127.0.0.1";
    String normalizedHost = UrlSchemaUtils.normalizeHost(hostWithDefaultPort);
    assertEquals("127.0.0.1:6570", normalizedHost);

    // Test host with non-default port
    String hostWithNonDefaultPort = "127.0.0.1:6666";
    String normalizedHost2 = UrlSchemaUtils.normalizeHost(hostWithNonDefaultPort);
    assertEquals("127.0.0.1:6666", normalizedHost2);
  }

  @Test
  public void testParseHosts() {
    // Test multiple hosts
    String hosts = "127.0.0.1:6570,127.0.0.2:6570";
    List<String> result = UrlSchemaUtils.parseHosts(hosts);
    assertEquals(2, result.size());
    assertTrue(result.contains("127.0.0.1:6570"));
    assertTrue(result.contains("127.0.0.2:6570"));

    // Test single host
    String singleHost = "127.0.0.1:6570";
    List<String> result2 = UrlSchemaUtils.parseHosts(singleHost);
    assertEquals(1, result2.size());
    assertTrue(result2.contains("127.0.0.1:6570"));
  }
}
