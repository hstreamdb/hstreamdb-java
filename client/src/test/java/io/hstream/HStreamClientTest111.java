package io.hstream;


import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HStreamClientTest111 {

  private static final Logger logger = LoggerFactory.getLogger(HStreamClientTest111.class);
  private static final String serviceUrl = "localhost:6570";
  private static final String TEST_STREAM_PREFIX = "TEST_STREAM_";
  private static final String TEST_SUBSCRIPTION_PREFIX = "TEST_SUB_";
  private HStreamClient client;
  private String testStreamName;
  private String testSubscriptionId;

  @BeforeEach
  public void setUp() {
    logger.info("enter setup");
    client = HStreamClient.builder().serviceUrl(serviceUrl).build();
    logger.info("create client done");
  }

  @Test
  void testCreateStream() {
    String suffix = RandomStringUtils.randomAlphanumeric(10);
    testStreamName = TEST_STREAM_PREFIX + suffix;
    client.createStream(testStreamName);
    logger.info("create stream done");
  }
}
