package ca.bc.gov.gbasites.controller;

import java.util.function.BiConsumer;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.range.Ranges;
import com.revolsys.io.CloseableResourceProxy;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.code.SimpleCodeTable;
import com.revolsys.spring.resource.Resource;

public class GbaSiteDatabase {

  private static CloseableResourceProxy<JdbcRecordStore> RECORD_STORE = CloseableResourceProxy
    .newProxy(GbaSiteDatabase::newRecordStore, JdbcRecordStore.class);

  public static JdbcRecordStore getRecordStore() {
    return RECORD_STORE.getResource();
  }

  private static JdbcRecordStore newRecordStore() {
    final JdbcRecordStore recordStore = GbaController.getGbaRecordStore();
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
