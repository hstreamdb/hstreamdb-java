package io.hstream;

import java.time.Instant;
import java.util.List;

public class Connector {
  String name;
  ConnectorType type;
  String target;
  Instant createdTime;
  String status;
  /** only available in GetConnector */
  String config;

  List<String> offsets;

  public static ConnectorBuilder newBuilder() {
    return new ConnectorBuilder();
  }

  public String getName() {
    return name;
  }

  public ConnectorType getType() {
    return type;
  }

  public String getTarget() {
    return target;
  }

  public Instant getCreatedTime() {
    return createdTime;
  }

  public String getStatus() {
    return status;
  }

  public String getConfig() {
    return config;
  }

  public List<String> getOffsets() {
    return offsets;
  }

  public static final class ConnectorBuilder {
    private String name;
    private ConnectorType type;
    private String target;
    private Instant createdTime;
    private String status;
    private String config;
    private List<String> offsets;

    public ConnectorBuilder name(String name) {
      this.name = name;
      return this;
    }

    public ConnectorBuilder type(ConnectorType type) {
      this.type = type;
      return this;
    }

    public ConnectorBuilder target(String target) {
      this.target = target;
      return this;
    }

    public ConnectorBuilder createdTime(Instant createdTime) {
      this.createdTime = createdTime;
      return this;
    }

    public ConnectorBuilder status(String status) {
      this.status = status;
      return this;
    }

    public ConnectorBuilder config(String config) {
      this.config = config;
      return this;
    }

    public ConnectorBuilder offsets(List<String> offsets) {
      this.offsets = offsets;
      return this;
    }

    public Connector build() {
      Connector connector = new Connector();
      connector.type = this.type;
      connector.config = this.config;
      connector.status = this.status;
      connector.name = this.name;
      connector.target = this.target;
      connector.offsets = this.offsets;
      connector.createdTime = this.createdTime;
      return connector;
    }
  }
}
