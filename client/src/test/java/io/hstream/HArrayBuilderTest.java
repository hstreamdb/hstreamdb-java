package io.hstream;

import static org.junit.jupiter.api.Assertions.*;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class HArrayBuilderTest {

  @Test
  public void testAddBoolean() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(true);
    HArray array = builder.build();
    assertTrue(array.getBoolean(0));
  }

  @Test
  public void testAddDouble() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(3.14);
    HArray array = builder.build();
    assertEquals(3.14, array.getDouble(0), 0.001);
  }

  @Test
  public void testAddInt() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(42);
    HArray array = builder.build();
    assertEquals(42, array.getInt(0));
  }

  @Test
  public void testAddLong() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(123456789L);
    HArray array = builder.build();
    assertEquals(123456789L, array.getLong(0));
  }

  @Test
  public void testAddString() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add("hello");
    HArray array = builder.build();
    assertEquals("hello", array.getString(0));
  }

  @Test
  public void testAddHRecord() {
    HRecord record = new HRecordBuilder().put("foo", "bar").build();
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(record);
    HArray array = builder.build();
    assertEquals(record, array.getHRecord(0));
  }

  @Test
  public void testEqStruct() {
    var x =
        Struct.newBuilder()
            .putFields("foo", Value.newBuilder().setStringValue("bar").build())
            .build();
    var y =
        Struct.newBuilder()
            .putFields("foo", Value.newBuilder().setStringValue("bar").build())
            .build();
    assertEquals(x, y);
  }

  @Test
  public void testAddHArray() {
    HArray innerArray = new HArrayBuilder().add(1).add(2).build();
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(innerArray);
    HArray outerArray = builder.build();
    assertEquals(innerArray, outerArray.getHArray(0));
  }

  @Test
  public void testAddMixedTypes() {
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(true);
    builder.add(42);
    builder.add(3.14);
    builder.add("hello");
    HArray array = builder.build();
    assertTrue(array.getBoolean(0));
    assertEquals(42, array.getInt(1));
    assertEquals(3.14, array.getDouble(2), 0.001);
    assertEquals("hello", array.getString(3));
  }

  @Test
  public void testAddMultipleHRecords() {
    HRecord record1 = new HRecordBuilder().put("foo", "bar").build();
    HRecord record2 = new HRecordBuilder().put("baz", 42).build();
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(record1);
    builder.add(record2);
    HArray array = builder.build();
    assertEquals(record1, array.getHRecord(0));
    assertEquals(record2, array.getHRecord(1));
  }

  @Test
  public void testAddNestedHArrays() {
    HArray innerArray1 = new HArrayBuilder().add("foo").add("bar").build();
    HArray innerArray2 = new HArrayBuilder().add(42).add(3.14).build();
    HArrayBuilder builder = new HArrayBuilder();
    builder.add(innerArray1);
    builder.add(innerArray2);
    HArray outerArray = builder.build();
    assertEquals(innerArray1, outerArray.getHArray(0));
    assertEquals(innerArray2, outerArray.getHArray(1));
  }
}
