package io.hstream.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.hstream.Reader;
import io.hstream.StreamShardOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReaderBuilderImplTest {

  private ReaderBuilderImpl builder;

  @BeforeEach
  public void setUp() {
    HStreamClientKtImpl mockClient = mock(HStreamClientKtImpl.class);
    builder = new ReaderBuilderImpl(mockClient);
  }

  @Test
  public void testBuild() {
    builder
        .streamName(UUID.randomUUID().toString())
        .shardId(0)
        .shardOffset(new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST))
        .timeoutMs(500)
        .readerId(UUID.randomUUID().toString())
        .requestTimeoutMs(10000);
    assertThrows(
        NullPointerException.class,
        () -> {
          // TODO, since we can mock more
          Reader reader = builder.build();
          assertNotNull(reader);
        });
  }

  @Test
  public void testBuildNullStreamName() {
    builder
        .shardId(0)
        .shardOffset(new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST))
        .timeoutMs(500)
        .readerId(UUID.randomUUID().toString())
        .requestTimeoutMs(10000);
    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  public void testBuildInvalidShardId() {
    builder
        .streamName(UUID.randomUUID().toString())
        .shardId(-1)
        .shardOffset(new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST))
        .timeoutMs(500)
        .readerId(UUID.randomUUID().toString())
        .requestTimeoutMs(10000);
    assertThrows(IllegalStateException.class, builder::build);
  }

  @Test
  public void testBuildNullShardOffset() {
    builder
        .streamName(UUID.randomUUID().toString())
        .shardId(0)
        .timeoutMs(500)
        .readerId(UUID.randomUUID().toString())
        .requestTimeoutMs(10000);
    assertThrows(NullPointerException.class, builder::build);
  }

  @Test
  public void testBuildInvalidRequestTimeoutMs() {
    builder
        .streamName(UUID.randomUUID().toString())
        .shardId(0)
        .shardOffset(new StreamShardOffset(StreamShardOffset.SpecialOffset.LATEST))
        .timeoutMs(500)
        .readerId(UUID.randomUUID().toString())
        .requestTimeoutMs(-10000);
    assertThrows(IllegalArgumentException.class, builder::build);
  }
}
