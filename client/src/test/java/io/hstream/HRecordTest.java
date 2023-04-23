package io.hstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HRecordTest {

  @Test
  public void test1() {
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 1).put("key2", false).put("key3", "hello").build();

    Assertions.assertEquals(1, hRecord.getInt("key1"));
    Assertions.assertFalse(hRecord.getBoolean("key2"));
    Assertions.assertEquals("hello", hRecord.getString("key3"));
  }

  @Test
  public void test2() {
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 1).put("key2", false).put("key3", "hello").build();

    HRecord parentHRecord = HRecord.newBuilder().put("record_key", hRecord).build();

    Assertions.assertEquals(1, parentHRecord.getHRecord("record_key").getInt("key1"));
    Assertions.assertFalse(parentHRecord.getHRecord("record_key").getBoolean("key2"));
    Assertions.assertEquals("hello", parentHRecord.getHRecord("record_key").getString("key3"));
  }
}
