package io.hstream;

import java.time.Instant;

public class Connector {
  String name;
  ConnectorType type;
  String target;
  Instant createdTime;
  String status;
  /** only available in GetConnector */
  String config;

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

  public static final class ConnectorBuilder {
    private String name;
    private ConnectorType type;
    private String target;
    private Instant createdTime;
    private String status;
    private String config;

    private ConnectorBuilder() {}

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

    public Connector build() {
      Connector connector = new Connector();
      connector.type = this.type;
      connector.status = this.status;
      connector.target = this.target;
      connector.createdTime = this.createdTime;
      connector.name = this.name;
      connector.config = this.config;
      return connector;
    }
  }
}
