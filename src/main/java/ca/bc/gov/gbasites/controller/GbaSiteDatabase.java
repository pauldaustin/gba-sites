package ca.bc.gov.gbasites.controller;

import java.util.function.BiConsumer;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.range.Ranges;
import com.revolsys.io.CloseableResourceProxy;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.code.SimpleCodeTable;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.spring.resource.Resource;

public class GbaSiteDatabase {

  private static CloseableResourceProxy<RecordStore> RECORD_STORE = CloseableResourceProxy
    .newProxy(GbaSiteDatabase::newRecordStore, RecordStore.class);

  public static RecordStore getRecordStore() {
    return RECORD_STORE.getResource();
  }

  public static JdbcRecordStore newRecordStore() {
    final JdbcRecordStore recordStore = (JdbcRecordStore)GbaController.getUserRecordStore();
    final SimpleCodeTable booleanCodeTable = new SimpleCodeTable(SitePoint.CIVIC_NUMBER_SUFFIX);
    for (final Object letter : Ranges.newRange('A', 'Z')) {
      booleanCodeTable.addValue(letter.toString(), letter.toString());
    }
    booleanCodeTable.addValue("1/2", "1/2");
    recordStore.addCodeTable(booleanCodeTable);

    return recordStore;
  }

  public static void setErrorHandler(final BiConsumer<Resource, Throwable> errorHandler) {
  }
}
