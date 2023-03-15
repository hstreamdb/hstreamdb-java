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

  @Test
  public void testBuildAndRetrieveValues() {
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 1).put("key2", false).put("key3", "hello").build();

    Assertions.assertEquals(1, hRecord.getInt("key1"));
    Assertions.assertFalse(hRecord.getBoolean("key2"));
    Assertions.assertEquals("hello", hRecord.getString("key3"));
  }

  @Test
  public void testNestedHRecord() {
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 1).put("key2", false).put("key3", "hello").build();

    HRecord parentHRecord = HRecord.newBuilder().put("record_key", hRecord).build();

    Assertions.assertEquals(1, parentHRecord.getHRecord("record_key").getInt("key1"));
    Assertions.assertFalse(parentHRecord.getHRecord("record_key").getBoolean("key2"));
    Assertions.assertEquals("hello", parentHRecord.getHRecord("record_key").getString("key3"));
  }

  @Test
  public void testNonexistentKey() {
    HRecord hRecord = HRecord.newBuilder().put("key1", 1).build();

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> hRecord.getInt("nonexistent_key"));
  }

  @Test
  public void testKeyExists() {
    HRecord hRecord = HRecord.newBuilder().put("key1", 1).build();

    Assertions.assertTrue(hRecord.contains("key1"));
    Assertions.assertFalse(hRecord.contains("nonexistent_key"));
  }

  @Test
  public void testGetKeys() {
    HRecord hRecord =
        HRecord.newBuilder().put("key1", 1).put("key2", false).put("key3", "hello").build();

    Assertions.assertArrayEquals(
        new String[] {"key1", "key2", "key3"}, hRecord.getKeySet().toArray());
  }
}
