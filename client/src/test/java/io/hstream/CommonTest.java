package io.hstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CommonTest {

  @Test
  public void testCompareRecordId() {
    RecordId rid1 = new RecordId(1, 100);
    RecordId rid2 = new RecordId(1, 50);
    RecordId rid3 = new RecordId(1, 180);
    RecordId rid4 = new RecordId(5, 50);
    RecordId rid5 = new RecordId(6, 50);
    RecordId rid6 = new RecordId(1, 180);
    RecordId rid7 = new RecordId(5, 50);
    RecordId rid8 = new RecordId(4, 50);
    Assertions.assertTrue(rid1.compareTo(rid2) > 0);
    Assertions.assertTrue(rid1.compareTo(rid3) < 0);
    Assertions.assertTrue(rid1.compareTo(rid4) < 0);
    Assertions.assertTrue(rid5.compareTo(rid4) > 0);
    Assertions.assertTrue(rid8.compareTo(rid4) < 0);
    Assertions.assertEquals(0, rid6.compareTo(rid3));
    Assertions.assertEquals(0, rid4.compareTo(rid7));
  }
}
