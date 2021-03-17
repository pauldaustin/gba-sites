package ca.bc.gov.gbasites.controller;

import java.util.function.BiConsumer;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.core.model.codetable.BoundaryCache;
import ca.bc.gov.gba.itn.GbaItnDatabase;
import ca.bc.gov.gba.itn.model.code.GbaItnCodeTables;
import ca.bc.gov.gba.itn.model.code.PartnerOrganization;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.range.Ranges;
import com.revolsys.io.CloseableResourceProxy;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.code.SingleValueCodeTable;
import com.revolsys.spring.resource.Resource;

public class GbaSiteDatabase {

  private static CloseableResourceProxy<JdbcRecordStore> RECORD_STORE = CloseableResourceProxy
    .newProxy(GbaSiteDatabase::newRecordStore, JdbcRecordStore.class);

  public static JdbcRecordStore getRecordStore() {
    return RECORD_STORE.getResource();
  }

  public static PartnerOrganization newPartnerOrganization(final String dataProvider) {
    final Identifier partnerOrganizationId = null;
    String name;
    String shortName;
    if (dataProvider.startsWith("Locality - ") || dataProvider.startsWith("Provider - ")
      || dataProvider.startsWith("Regional District - ")) {
      name = dataProvider;
      shortName = dataProvider.substring(dataProvider.indexOf('-') + 2);
    } else {
      final BoundaryCache localities = GbaItnCodeTables.getLocalities();
      final Identifier localityId = localities.getIdentifier(dataProvider);
      if (localityId == null) {
        final BoundaryCache regionalDistricts = GbaItnCodeTables.getRegionalDistricts();
        final Identifier regionalDistrictId = regionalDistricts.getIdentifier(dataProvider);
        if (regionalDistrictId == null) {
          shortName = dataProvider;
          name = "Provider - " + dataProvider;
        } else {
          shortName = regionalDistrictId.getString(0);
          name = "Regional District - " + regionalDistrictId;
        }
      } else {
        shortName = localities.getValue(localityId);
        name = "Locality - " + shortName;
      }
      // TODO disabled so we don't update the database
      // final Identifier partnerOrganizationId =
      // PartnerOrganizations.getId(name);
    }
    return new PartnerOrganization(partnerOrganizationId, name, shortName);
  }

  private static JdbcRecordStore newRecordStore() {
    final JdbcRecordStore recordStore = GbaItnDatabase.getRecordStore();
    final SingleValueCodeTable booleanCodeTable = new SingleValueCodeTable(
      SitePoint.CIVIC_NUMBER_SUFFIX);
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
