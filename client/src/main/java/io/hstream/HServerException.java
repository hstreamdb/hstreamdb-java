package io.hstream;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

public class HServerException extends RuntimeException {
  ErrBody errBody;

  static class ErrBody {
    int error;
    String message;
    JsonElement extra;
  }

  HServerException(ErrBody errBody) {
    this.errBody = errBody;
  }

  @Override
  public String getMessage() {
    return getErrorMessage();
  }

  public String getErrorMessage() {
    return errBody.message;
  }

  public String getRawErrorBody() {
    return new Gson().toJson(errBody);
  }

  public static HServerException tryToHServerException(String errBodyStr) {
    try {
      var errBody = new Gson().fromJson(errBodyStr, ErrBody.class);
      if (errBody == null) {
        return null;
      }
      return new HServerException(errBody);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}
