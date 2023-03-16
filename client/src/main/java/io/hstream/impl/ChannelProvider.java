package io.hstream.impl;

import io.grpc.ManagedChannel;
import java.io.Closeable;

public interface ChannelProvider extends Closeable {
  ManagedChannel get(String serverUrl);
}
