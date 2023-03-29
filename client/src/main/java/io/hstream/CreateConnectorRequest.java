package io.hstream;

import static com.google.common.base.Preconditions.checkArgument;

public class CreateConnectorRequest {
  String name;
  ConnectorType type;
  String target;
  String config;

  public static CreateConnectorRequestBuilder newBuilder() {
    return new CreateConnectorRequestBuilder();
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

  public String getConfig() {
    return config;
  }

  public static final class CreateConnectorRequestBuilder {
    private String name;
    private ConnectorType type;
    private String target;
    private String config;

    private CreateConnectorRequestBuilder() {}

    public CreateConnectorRequestBuilder name(String name) {
      this.name = name;
      return this;
    }

    public CreateConnectorRequestBuilder type(ConnectorType type) {
      this.type = type;
      return this;
    }

    public CreateConnectorRequestBuilder target(String target) {
      this.target = target;
      return this;
    }

    public CreateConnectorRequestBuilder config(String config) {
      this.config = config;
      return this;
    }

    public CreateConnectorRequest build() {
      checkArgument(name != null, "connector name should not be null");
      checkArgument(type != null, "connector type should not be null");
      checkArgument(target != null, "connector target should not be null");
      checkArgument(config != null, "connector config should not be null");
      CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest();
      createConnectorRequest.config = this.config;
      createConnectorRequest.type = this.type;
      createConnectorRequest.name = this.name;
      createConnectorRequest.target = this.target;
      return createConnectorRequest;
    }
  }
}
