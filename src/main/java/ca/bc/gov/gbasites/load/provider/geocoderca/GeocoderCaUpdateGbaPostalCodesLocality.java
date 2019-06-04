package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.controller.ArchiveAndChangeLogController;
import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.IntegrationAction;
import ca.bc.gov.gba.model.type.code.Localities;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.set.Sets;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class GeocoderCaUpdateGbaPostalCodesLocality implements SitePoint, Cancellable {

  private final GeocoderCaImportSites importSites;

  private final Path inputFile;

  private final LabelCountMapTableModel counts;

  private final String localityName;

  private final Identifier localityId;

  private final ArchiveAndChangeLogController archiveAndChangeLog;

  public GeocoderCaUpdateGbaPostalCodesLocality(
    final GeocoderCaUpdateGbaPostalCodes geocoderCaUpdateGbaPostalCodes,
    final GeocoderCaImportSites importSites, final Path path) {
    this.importSites = importSites;
    this.archiveAndChangeLog = importSites.getArchiveAndChangeLog();
    this.inputFile = path;
    this.counts = geocoderCaUpdateGbaPostalCodes.counts;
    final String fileName = Paths.getBaseName(path);
    final String localityFileName = fileName.replace("_SITE_POINT_GEOCODER_CA", "");
    this.localityName = Localities.LOCALITY_NAME_BY_FILE_NAME.get(localityFileName);
    this.localityId = Localities.LOCALITY_ID_BY_FILE_NAME.get(localityFileName);
  }

  private String findPostalCode(final Map<Integer, Set<String>> postalCodesByNumber,
    final int civicNumber, final int step) {
    final int min = Math.floorDiv(civicNumber, 100) * 100;

    final int max = min + 99;
    for (int number = civicNumber + step; min <= number && number <= max; number += step) {
      final Set<String> postalCodes = postalCodesByNumber.get(number);
      if (postalCodes != null && postalCodes.size() == 1) {
        return postalCodes.iterator().next();
      }
    }
    return null;
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  private Map<String, Map<Integer, Set<String>>> loadPostalCodes() {
    final Map<String, Map<Integer, Set<String>>> postalCodeByStreetAndNumber = new HashMap<>();
    try (
      RecordReader reader = RecordReader.newRecordReader(this.inputFile)) {
      for (final Record site : cancellable(reader)) {
        this.counts.addCount(this.localityName, BatchUpdateDialog.READ);
        final String streetName = site.getString(STREET_NAME);
        final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
        String postalCode = site.getString(POSTAL_CODE);
        if (postalCode.length() == 6) {
          postalCode = postalCode.substring(0, 3) + " " + postalCode.substring(3);
        }
        if (civicNumber != null && postalCode != null && streetName != null) {
          Maps.addToCollection(Sets.treeFactory(), postalCodeByStreetAndNumber, streetName,
            civicNumber, postalCode);
        }
      }
    }
    return postalCodeByStreetAndNumber;
  }

  public void run() {
    final Map<String, Map<Integer, Set<String>>> postalCodeByStreetAndNumber = loadPostalCodes();
    updateGbaSites(postalCodeByStreetAndNumber);
  }

  @Override
  public String toString() {
    return this.localityName;
  }

  private void updateGbaSites(
    final Map<String, Map<Integer, Set<String>>> postalCodeByStreetAndNumber) {
    final RecordStore recordStore = GbaController.getGbaRecordStore();
    final Query query = new Query(SiteTables.SITE_POINT) //
      .setWhereCondition(Q.equal(LOCALITY_ID, this.localityId))//
    ;
    try (
      RecordReader reader = recordStore.getRecords(query)) {
      for (final Record site : cancellable(reader)) {
        this.counts.addCount(this.localityName, "GBA Read");
        updateSite(postalCodeByStreetAndNumber, site);
      }
    }
  }

  private void updateSite(final Map<String, Map<Integer, Set<String>>> postalCodeByStreetAndNumber,
    final Record site) {
    final String streetName = site.getString(STREET_NAME);
    final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
    final String postalCode = site.getString(POSTAL_CODE);
    if (civicNumber != null) {
      String newPostalCode = postalCode;
      final Map<Integer, Set<String>> postalCodesByNumber = Maps.getMap(postalCodeByStreetAndNumber,
        streetName);
      final Collection<String> geocoderPostalCodes = postalCodesByNumber.get(civicNumber);
      if (geocoderPostalCodes != null) {
        if (geocoderPostalCodes.size() == 1) {
          newPostalCode = geocoderPostalCodes.iterator().next();
        }
      }
      if (newPostalCode == null) {
        String previousPostalCode = findPostalCode(postalCodesByNumber, civicNumber, -2);
        if (previousPostalCode != null && geocoderPostalCodes != null
          && !geocoderPostalCodes.contains(previousPostalCode)) {
          previousPostalCode = null;
        }
        String nextPostalCode = findPostalCode(postalCodesByNumber, civicNumber, 2);
        if (nextPostalCode != null && geocoderPostalCodes != null
          && !geocoderPostalCodes.contains(nextPostalCode)) {
          nextPostalCode = null;
        }
        if (previousPostalCode == null) {
          if (nextPostalCode != null) {
            newPostalCode = nextPostalCode;
          }
        } else if (nextPostalCode == null || previousPostalCode.equals(nextPostalCode)) {
          newPostalCode = previousPostalCode;
        }
        if (newPostalCode == null && geocoderPostalCodes != null) {
          newPostalCode = geocoderPostalCodes.iterator().next();
        }
      }
      if (newPostalCode != null && !newPostalCode.equals(postalCode)) {
        final MapEx newValues = new LinkedHashMapEx()//
          .add(POSTAL_CODE, newPostalCode)//
        ;

        this.archiveAndChangeLog.updateRecord(site, newValues, IntegrationAction.UPDATE_FIELD);
        this.counts.addCount(this.localityName, BatchUpdateDialog.UPDATED);
      }
    }
  }
}
