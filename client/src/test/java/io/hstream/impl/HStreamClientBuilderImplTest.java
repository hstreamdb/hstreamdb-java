package io.hstream.impl;

import static org.junit.jupiter.api.Assertions.*;

import io.hstream.HStreamDBClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class HStreamClientBuilderImplTest {
  private HStreamClientBuilderImpl builder;

  @BeforeEach
  public void setUp() {
    builder = new HStreamClientBuilderImpl();
  }

  @Test
  public void testServiceUrl() {
    builder.serviceUrl("hstream://localhost:6570");
    assertEquals("hstream://localhost:6570", builder.serviceUrl);
  }

  @Test
  public void testEnableTls() {
    builder.enableTls();
    assertTrue(builder.enableTls);
  }

  @Test
  public void testTlsCaPath() {
    builder.tlsCaPath("/path/to/ca.pem");
    assertEquals("/path/to/ca.pem", builder.caPath);
  }

  @Test
  public void testEnableTlsAuthentication() {
    builder.enableTlsAuthentication();
    assertTrue(builder.enableTlsAuthentication);
  }

  @Test
  public void testTlsKeyPath() {
    builder.tlsKeyPath("/path/to/key.pem");
    assertEquals("/path/to/key.pem", builder.keyPath);
  }

  @Test
  public void testTlsCertPath() {
    builder.tlsCertPath("/path/to/cert.pem");
    assertEquals("/path/to/cert.pem", builder.certPath);
  }

  @Test
  public void testRequestTimeoutMs() {
    builder.requestTimeoutMs(1000L);
    assertEquals(1000L, builder.requestTimeoutMs);
  }

  @Test
  public void testBuildWithNullServiceUrl() {
    assertThrows(NullPointerException.class, () -> builder.build());
  }

  @Test
  public void testBuildWithNegativeRequestTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.serviceUrl("hstream://localhost:6570").requestTimeoutMs(-1L).build());
  }

  @Test
  public void testBuildWithHStreamSchemaAndNoTls() {
    assertThrows(
        HStreamDBClientException.class,
        () -> builder.serviceUrl("hstreams://localhost:6570").build());
  }

  @Test
  public void testBuildWithTlsAndNullCaPath() {
    assertThrows(
        NullPointerException.class,
        () -> builder.serviceUrl("hstreams://localhost:6570").enableTls().build());
  }

  @Test
  public void testBuildWithTlsAndInvalidCaPath() {
    assertThrows(
        HStreamDBClientException.class,
        () -> builder.serviceUrl("hstreams://localhost:6570").enableTls().tlsCaPath("").build());
  }

  @Test
  public void testBuildWithTlsAuthenticationAndNullKeyPath() {
    assertThrows(
        NullPointerException.class,
        () ->
            builder
                .serviceUrl("hstreams://localhost:6570")
                .enableTls()
                .enableTlsAuthentication()
                .tlsCertPath("/path/to/cert.pem")
                .build());
  }
}
