package io.hstream.util;

import io.hstream.HStreamDBClientException;
import io.hstream.UrlSchema;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class UrlSchemaUtils {
  public static Pair<UrlSchema, List<String>> parseServerUrls(String url) {
    String uriStr = url.strip();
    var schemaHosts = uriStr.split("://");
    if (schemaHosts.length != 2) {
      throw new HStreamDBClientException(
          "incorrect serviceUrl: " + uriStr + " (correct example: hstream://127.0.0.1:6570)");
    }
    var schemaStr = schemaHosts[0];
    UrlSchema urlSchema;
    try {
      urlSchema = UrlSchema.valueOf(schemaStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new HStreamDBClientException("Invalid url schema:" + schemaStr);
    }
    var hosts = schemaHosts[1];
    return Pair.of(urlSchema, parseHosts(hosts));
  }

  static List<String> parseHosts(String hosts) {
    return Arrays.stream(hosts.split(","))
        .map(UrlSchemaUtils::normalizeHost)
        .collect(Collectors.toList());
  }

  static String normalizeHost(String host) {
    var address_port = host.split(":");
    if (address_port.length == 1) {
      return host + ":6570";
    }
    return host;
  }
}
