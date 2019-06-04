package ca.bc.gov.gbasites.load.common;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.type.TransportLine;
import ca.bc.gov.gba.process.qa.AbstractTaskByLocalityProcess;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Debug;
import com.revolsys.util.Pair;
import com.revolsys.util.Property;

public class LoadProviderSitesIntoGba extends AbstractTaskByLocalityProcess implements SitePoint {
  public static final String TO_DELETE = "To Delete";

  private static final List<String> EXCLUDE_FIELD_NAMES = Arrays.asList(SITE_ID,
    CREATE_INTEGRATION_SESSION_ID, MODIFY_INTEGRATION_SESSION_ID, CUSTODIAN_SESSION_ID,
    ADDRESS_COMMENT, EXCLUDED_RULES);

  private static final String LOCALITY = "Locality";

  public static final String PROVIDER_READ = "Provider Read";

  public static final String MATCHED = "Matched";

  private static final Map<String, Map<String, Integer>> localitiesWithNoSites = new TreeMap<>();

  public static String toFullAddress(final String unitDescriptor, final String civicNumber,
    final String civicNumberSuffix, final String name) {
    final StringBuilder fullAddress = new StringBuilder();
    if (Property.hasValue(unitDescriptor)) {
      fullAddress.append(unitDescriptor);
      fullAddress.append('\n');
    }
    if (Property.hasValue(civicNumber)) {
      fullAddress.append(civicNumber);
      if (Property.hasValue(civicNumberSuffix)) {
        final char suffixFirstChar = civicNumberSuffix.charAt(0);
        if (suffixFirstChar >= '0' && suffixFirstChar <= '9') {
          fullAddress.append(' ');
        }
        fullAddress.append(civicNumberSuffix.toUpperCase());
      }
      fullAddress.append(' ');
    } else if (Property.hasValue(civicNumberSuffix)) {
      fullAddress.append(civicNumberSuffix.toUpperCase());
      fullAddress.append(' ');
    }
    if (Property.hasValue(name)) {
      fullAddress.append(name);
    }
    if (fullAddress.length() > 0) {
      final char lastChar = fullAddress.charAt(fullAddress.length() - 1);
      if (lastChar == ' ' || lastChar == '\n') {
        fullAddress.deleteCharAt(fullAddress.length() - 1);
      }
    }
    return fullAddress.toString();
  }

  public static void writeNoSites() {
    final RecordDefinition recordDefinition = new RecordDefinitionBuilder("NO_SITES")//
      .addField("REGIONAL_DISTRICT_NAME", DataTypes.STRING, 42) //
      .addField("LOCALITY_NAME", DataTypes.STRING, 28) //
      .addField("TRANSPORT_LINE_COUNT", DataTypes.INT) //
      .getRecordDefinition();
    final Path file = ImportSites.SITES_DIRECTORY.resolve("NO_SITES.xlsx");
    try (
      final RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file)) {
      for (final Entry<String, Map<String, Integer>> regionalDistrictEntry : ImportSites.dialog
        .cancellable(localitiesWithNoSites.entrySet())) {
        final String regionalDistrictName = regionalDistrictEntry.getKey();
        for (final Entry<String, Integer> localityEntry : ImportSites.dialog
          .cancellable(regionalDistrictEntry.getValue().entrySet())) {
          final String localityName = localityEntry.getKey();
          final Integer transportLineCount = localityEntry.getValue();
          writer.write(regionalDistrictName, localityName, transportLineCount);
        }
      }
    }
  }

  private final ImportSites dialog;

  public LoadProviderSitesIntoGba(final ImportSites dialog) {
    super(dialog);
    this.dialog = dialog;
  }

  private void addLocalityCount(final String localityName, final String counterName) {
    ImportSites.dialog.addLabelCount(LOCALITY, localityName, counterName);
  }

  private Map<String, List<Record>> getSitesByAddress(final List<Record> records,
    final List<Record> nonMatchedRecords) {
    final Map<String, List<Record>> sitesByAddress = new HashMap<>();
    for (final Record record : cancellable(records)) {
      final String address = record.getString(FULL_ADDRESS);
      if (address == null) {
        nonMatchedRecords.add(record);
      } else {
        Maps.addToList(sitesByAddress, address, record);
      }
    }
    return sitesByAddress;
  }

  private Map<Identifier, List<Record>> getSitesByCustodianSiteId(final List<Record> records,
    final List<Record> nonMatchedSites) {
    final Map<Identifier, List<Record>> sitesByCustodianSiteId = new HashMap<>();
    for (final Record record : cancellable(records)) {
      final Identifier custodianSiteId = record.getIdentifier(CUSTODIAN_SITE_ID);
      if (custodianSiteId == null) {
        nonMatchedSites.add(record);
      } else {
        Maps.addToList(sitesByCustodianSiteId, custodianSiteId, record);
      }
    }
    return sitesByCustodianSiteId;
  }

  private Map<Identifier, List<Record>> getSitesFromGba(final Identifier localityId,
    final String localityName) {
    final Map<Identifier, List<Record>> recordsByCustodianId = new TreeMap<>(
      GbaController.getPartnerOrganizations());
    final Query sitesQuery = new Query(SiteTables.SITE_POINT, Q.equal(LOCALITY_ID, localityId));
    sitesQuery.setCancellable(this);
    final RecordStore recordStore = getRecordStore();
    try (
      RecordReader reader = recordStore.getRecords(sitesQuery)) {
      for (final Record record : reader) {
        addLocalityCount(localityName, BatchUpdateDialog.GBA_READ);
        Identifier custodianOrgId = record.getIdentifier(CUSTODIAN_PARTNER_ORG_ID);
        if (custodianOrgId == null) {
          custodianOrgId = Identifier.newIdentifier(3);
        }
        Maps.addToList(recordsByCustodianId, custodianOrgId, record);
      }
      return recordsByCustodianId;
    }
  }

  private Map<Identifier, List<Record>> getSitesFromProvider(final Identifier localityId,
    final String localityName) {
    final Map<Identifier, List<Record>> recordsByCustodianId = new TreeMap<>(
      GbaController.getPartnerOrganizations());
    final String localityFileName = BatchUpdateDialog.toFileName(localityName);
    final String prefix = localityFileName + "_SITE_POINT";
    final String filePattern = prefix + "*.tsv";
    try (
      DirectoryStream<Path> files = Files.newDirectoryStream(ImportSites.LOCALITY_DIRECTORY,
        filePattern)) {
      files.forEach((dataProviderSiteFile) -> {
        final String baseName = Paths.getBaseName(dataProviderSiteFile);
        final String providerName = baseName.substring(prefix.length());
        final Identifier partnerOrganizationId;
        if (Property.isEmpty(providerName)) {
          partnerOrganizationId = this.dialog.getPartnerOrganizationByShortName(localityFileName);
        } else {
          partnerOrganizationId = this.dialog
            .getPartnerOrganizationByShortName(providerName.substring(1));
        }
        if (partnerOrganizationId == null) {
          Logs.error(this, "Unknown Partner Organization for file:" + dataProviderSiteFile);
        } else {
          try (
            RecordReader reader = RecordReader.newRecordReader(dataProviderSiteFile)) {
            final List<Record> records = new ArrayList<>();
            for (final Record record : reader) {
              addLocalityCount(localityName, PROVIDER_READ);
              records.add(record);
            }
            recordsByCustodianId.put(partnerOrganizationId, records);
          }
        }
      });
    } catch (final IOException e) {
    } finally {
      logNoSites(localityId, localityName, recordsByCustodianId);
    }
    return recordsByCustodianId;
  }

  private void insertRecords(final String localityName, final PathName pathName,
    final List<? extends Map<String, ? extends Object>> records) {
    for (final Map<String, ? extends Object> record : records) {
      addLocalityCount(localityName, BatchUpdateDialog.INSERTED);
      this.dialog.insertRecord(null, pathName, record);
    }
  }

  private void loadSites() {
    final Map<Identifier, List<Record>> gbaSitesByCustodianId = getSitesFromGba(this.localityId,
      this.localityName);

    final Map<Identifier, List<Record>> providerSitesByCustodianId = getSitesFromProvider(
      this.localityId, this.localityName);

    if (providerSitesByCustodianId.isEmpty()) {
      // Nothing to do, no new data to import.
    } else if (gbaSitesByCustodianId.isEmpty()) {
      loadSitesInitialImport(this.localityName, providerSitesByCustodianId);
    } else {
      final List<Record> gbaNonMatchedSites = new ArrayList<>();
      final List<Record> providerNonMatchedSites = new ArrayList<>();

      loadSitesCustodians(this.localityName, gbaSitesByCustodianId, gbaNonMatchedSites,
        providerSitesByCustodianId, providerNonMatchedSites);

      if (gbaNonMatchedSites.isEmpty()) {
        if (!providerNonMatchedSites.isEmpty()) {
          // Insert new records
          insertRecords(this.localityName, SiteTables.SITE_POINT, providerNonMatchedSites);
        }
      } else if (!providerNonMatchedSites.isEmpty()) {
        // TODO match by address
        Debug.noOp();
      }
    }
  }

  private void loadSitesCustodian(final String localityName, final Identifier custodianOrgId,
    final List<Record> gbaCustodianSites, final List<Record> providerCustodianSites) {
    if (Property.isEmpty(gbaCustodianSites)) {
      insertRecords(this.localityName, SiteTables.SITE_POINT, providerCustodianSites);
    } else {
      final Pair<List<Record>, List<Record>> siteIdNonMatchedSites = loadSitesCustodianSiteId(
        localityName, gbaCustodianSites, providerCustodianSites);

      loadSitesCustodianAddress(localityName, custodianOrgId, siteIdNonMatchedSites.getValue1(),
        siteIdNonMatchedSites.getValue2());
    }
  }

  private void loadSitesCustodianAddress(final String localityName, final Identifier custodianOrgId,
    final List<Record> gbaCustodianSites, final List<Record> providerCustodianSites) {
    final List<Record> gbaNonMatchedSites = new ArrayList<>();
    final List<Record> providerNonMatchedSites = new ArrayList<>();
    final Map<String, List<Record>> gbaSitesByCustodianSiteId = getSitesByAddress(gbaCustodianSites,
      gbaNonMatchedSites);
    final Map<String, List<Record>> providerSitesByCustodianSiteId = getSitesByAddress(
      providerCustodianSites, providerNonMatchedSites);
    for (final Entry<String, List<Record>> recordEntry : this.dialog
      .cancellable(providerSitesByCustodianSiteId.entrySet())) {
      final String address = recordEntry.getKey();
      final List<Record> providerSites = recordEntry.getValue();
      final List<Record> gbaSites = gbaSitesByCustodianSiteId.remove(address);
      if (Property.isEmpty(gbaSites)) {
        insertRecords(this.localityName, SiteTables.SITE_POINT, providerSites);
      } else {
        if (providerSites.size() == 1 && gbaSites.size() == 1) {
          final Record providerRecord = providerSites.get(0);
          final Record gbaRecord = gbaSites.get(0);
          matchSite(localityName, providerRecord, gbaRecord);
        } else {
          // TODO match with different point locations
          for (final Record providerRecord : providerSites) {
            boolean matched = false;
            for (final Iterator<Record> gbaIterator = gbaSites.iterator(); gbaIterator.hasNext()
              && !matched;) {
              final Record gbaRecord = gbaIterator.next();

              final List<String> differentFieldNames = gbaRecord
                .getDifferentFieldNamesExclude(providerRecord, EXCLUDE_FIELD_NAMES);

              if (differentFieldNames.isEmpty()) {
                addLocalityCount(localityName, MATCHED);
                matched = true;
                gbaIterator.remove();
              }
            }
          }
        }

        // TODO compare values
      }
    }
    loadSitesCustodianLogToDelete(localityName, custodianOrgId, gbaSitesByCustodianSiteId);
  }

  public void loadSitesCustodianLogToDelete(final String localityName,
    final Identifier custodianOrgId, final Map<String, List<Record>> gbaSitesByCustodianSiteId) {
    final String partnerOrganizationShortName = ProviderSitePointConverter
      .getPartnerOrganizationShortName(custodianOrgId);

    final Path deleteDirectory = ImportSites.SITES_DIRECTORY.resolve("ToDelete");
    Paths.createDirectories(deleteDirectory);
    String filePrefix = BatchUpdateDialog.toFileName(partnerOrganizationShortName);
    if (!partnerOrganizationShortName.equalsIgnoreCase(localityName)) {
      filePrefix += "_" + BatchUpdateDialog.toFileName(localityName);
    }
    filePrefix += "_DELETE";
    if (gbaSitesByCustodianSiteId.isEmpty()) {
      Paths.deleteFiles(deleteDirectory, filePrefix + ".*");
    } else {
      final Path deleteFile = deleteDirectory.resolve(filePrefix + ".tsv");
      final RecordDefinition recordDefinition = GbaController.getUserRecordStore()
        .getRecordDefinition(SiteTables.SITE_POINT);
      try (
        RecordWriter deleteWriter = RecordWriter.newRecordWriter(recordDefinition, deleteFile)) {
        for (final List<Record> records : gbaSitesByCustodianSiteId.values()) {
          for (final Record record : records) {
            deleteWriter.write(record);
            this.dialog.addLabelCount(LOCALITY, localityName, TO_DELETE);
          }
        }
      }
    }
  }

  private void loadSitesCustodians(final String localityName,
    final Map<Identifier, List<Record>> gbaSitesByCustodianId,
    final List<Record> gbaNonMatchedSites,
    final Map<Identifier, List<Record>> providerSitesByCustodianId,
    final List<Record> providerNonMatchedSites) {
    for (final Identifier custodianOrgId : cancellable(
      new ArrayList<>(providerSitesByCustodianId.keySet()))) {
      final List<Record> providerCustodianSites = providerSitesByCustodianId.remove(custodianOrgId);
      final List<Record> gbaCustodianSites = gbaSitesByCustodianId.remove(custodianOrgId);
      try {
        ImportSites.custodianPartnerOrgIdForThread.set(custodianOrgId);
        loadSitesCustodian(localityName, custodianOrgId, gbaCustodianSites, providerCustodianSites);
      } finally {
        ImportSites.custodianPartnerOrgIdForThread.set(null);
      }
    }
    providerSitesByCustodianId.clear();
    for (final List<Record> records : gbaSitesByCustodianId.values()) {
      gbaNonMatchedSites.addAll(records);
    }
    gbaSitesByCustodianId.clear();
  }

  private Pair<List<Record>, List<Record>> loadSitesCustodianSiteId(final String localityName,
    final List<Record> gbaCustodianSites, final List<Record> providerCustodianSites) {
    final List<Record> gbaNonMatchedSites = new ArrayList<>();
    final List<Record> providerNonMatchedSites = new ArrayList<>();
    final Map<Identifier, List<Record>> gbaSitesByCustodianSiteId = getSitesByCustodianSiteId(
      gbaCustodianSites, gbaNonMatchedSites);
    gbaCustodianSites.clear();
    final Map<Identifier, List<Record>> providerSitesByCustodianSiteId = getSitesByCustodianSiteId(
      providerCustodianSites, providerNonMatchedSites);
    providerCustodianSites.clear();
    for (final Entry<Identifier, List<Record>> recordEntry : this.dialog
      .cancellable(providerSitesByCustodianSiteId.entrySet())) {
      final Identifier custodianSiteId = recordEntry.getKey();
      final List<Record> providerSites = recordEntry.getValue();
      final List<Record> gbaSites = gbaSitesByCustodianSiteId.remove(custodianSiteId);
      if (Property.isEmpty(gbaSites)) {
        // Attempt to match later by address
        providerNonMatchedSites.addAll(providerSites);
      } else {
        if (providerSites.size() == 1 && gbaSites.size() == 1) {
          final Record providerRecord = providerSites.get(0);
          final Record gbaRecord = gbaSites.get(0);
          matchSite(localityName, providerRecord, gbaRecord);
        } else {
          for (final Record providerRecord : providerSites) {
            boolean matched = false;
            for (final Iterator<Record> gbaIterator = gbaSites.iterator(); gbaIterator.hasNext()
              && !matched;) {
              final Record gbaRecord = gbaIterator.next();

              final List<String> differentFieldNames = gbaRecord
                .getDifferentFieldNamesExclude(providerRecord, EXCLUDE_FIELD_NAMES);

              if (differentFieldNames.isEmpty()) {
                addLocalityCount(localityName, MATCHED);
                matched = true;
                gbaIterator.remove();
              }
            }
          }
        }

        // TODO compare values
      }
    }
    for (final List<Record> records : gbaSitesByCustodianSiteId.values()) {
      gbaNonMatchedSites.addAll(records);
    }
    return new Pair<>(gbaNonMatchedSites, providerNonMatchedSites);
  }

  /**
   * For the first import for a locality insert all the sites rather than doing a merge.
   *
   * @param localityName
   * @param sitesByCustodianSiteIdAndCustodianId
   */
  private void loadSitesInitialImport(final String localityName,
    final Map<Identifier, List<Record>> sitesByCustodianId) {
    final Identifier intgrationSessionId = getIntegrationSessionId();
    final RecordStore recordStore = getRecordStore();
    final RecordDefinition recordDefinition = recordStore
      .getRecordDefinition(SiteTables.SITE_POINT);
    try (
      RecordWriter writer = recordStore.newRecordWriter()) {
      for (final List<Record> records : sitesByCustodianId.values()) {
        for (final Record record : records) {
          addLocalityCount(localityName, BatchUpdateDialog.INSERTED);
          final Record writeRecord = recordDefinition.newRecord(record);
          writeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, intgrationSessionId);
          writeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, intgrationSessionId);
          writeRecord.setValue(CUSTODIAN_SESSION_ID, intgrationSessionId);
          writer.write(writeRecord);
        }
      }
    }
  }

  private void logNoSites(final Identifier localityId, final String localityName,
    final Map<?, ?> siteMap) {
    if (siteMap.isEmpty()) {
      final Geometry localityGeometry = this.localityCache.getBoundaryGeometry(localityId);
      final Point localityCentroid = localityGeometry.getPointWithin();
      final BoundaryCache regionalDistricts = GbaController.getRegionalDistricts();
      final Identifier regionalDistrictId = regionalDistricts.getBoundaryId(localityCentroid);
      final Record regionalDistrict = regionalDistricts.getRecord(regionalDistrictId);
      if ("BC".equals(regionalDistrict.getString("REGION_CODE"))) {
        final Query query = new Query(GbaTables.TRANSPORT_LINE);
        query.setWhereCondition(Q.equal(TransportLine.LEFT_LOCALITY_ID, localityId));
        query.or(Q.equal(TransportLine.RIGHT_LOCALITY_ID, localityId));
        final RecordStore recordStore = getRecordStore();
        final int transportLineCount = recordStore.getRecordCount(query);
        if (transportLineCount > 0) {
          final String regionalDistrictName = regionalDistrict.getString("NAME");
          Maps.addToMap(localitiesWithNoSites, regionalDistrictName, localityName,
            transportLineCount);
        }
      }
    }
  }

  private void matchSite(final String localityName, final Record providerRecord,
    final Record gbaRecord) {
    final Identifier custodianSessionId = gbaRecord.getIdentifier(CUSTODIAN_SESSION_ID);
    final List<String> differentFieldNames = gbaRecord.getDifferentFieldNamesExclude(providerRecord,
      EXCLUDE_FIELD_NAMES);

    if (differentFieldNames.equals(Collections.singletonList(CUSTODIAN_SITE_ID))) {
      final Map<String, Object> newValues = Maps.newLinkedHash(gbaRecord);
      newValues.put(CUSTODIAN_SITE_ID, providerRecord.getValue(CUSTODIAN_SITE_ID));
      this.dialog.updateRecord(null, gbaRecord, newValues);
      addLocalityCount(localityName, BatchUpdateDialog.UPDATED);
    } else if (gbaRecord.equalValue(MODIFY_INTEGRATION_SESSION_ID, custodianSessionId)) {
      // If the record was last modified by the custodian then update the record
      if (this.dialog.updateRecord(null, gbaRecord, providerRecord)) {
        addLocalityCount(localityName, BatchUpdateDialog.UPDATED);
      } else {
        addLocalityCount(localityName, MATCHED);
      }
    } else {
      if (gbaRecord.compareValue(providerRecord, CAPTURE_DATE) < 0) {
        differentFieldNames.add(CAPTURE_DATE);
      }
      if (differentFieldNames.isEmpty()) {
        addLocalityCount(localityName, MATCHED);
      } else { // TODO update values?
        Debug.noOp();
      }
    }
  }

  @Override
  protected boolean processLocality() {
    if (!this.localityCache.getIdentifier(this.localityName).equals(this.localityId)) {
      return true;
    } else {
      final RecordStore recordStore = getRecordStore();
      try (
        Transaction transaction = recordStore.newTransaction(Propagation.REQUIRES_NEW)) {
        loadSites();

        // for (final Entry<Identifier, Map<SiteKey, List<Record>>> sitesEntry
        // :
        // cancellable(
        // sitesByStreetName.entrySet())) {
        // final Identifier streetNameId = sitesEntry.getKey();
        // final Map<SiteKey, List<Record>> sitesByKey =
        // sitesEntry.getValue();
        // final Map<SiteKey, List<Record>> providerSitesByKey =
        // sitesByCustodianSiteIdAndCustodianId
        // .remove(streetNameId);
        // if (Property.hasValue(providerSitesByKey)) {
        // for (final Entry<SiteKey, List<Record>> sitesByKeyEntry :
        // cancellable(
        // sitesByKey.entrySet())) {
        // final SiteKey siteKey = sitesByKeyEntry.getKey();
        // final List<Record> sites = sitesByKeyEntry.getValue();
        // final List<Record> providerSites =
        // providerSitesByKey.remove(siteKey);
        // if (Property.hasValue(providerSites)) {
        // // TODO compare sites with provider sites
        // } else {
        // // No provider sites for street
        // }
        // }
        // insertNewSitesStreet(localityName, providerSitesByKey);
        // } else {
        // // No provider sites for street
        // }
        // }
        // // Cleanup memory as quick as possible
        // sitesByStreetName.clear();
        //
        // insertNewSitesStreets(localityName,
        // sitesByCustodianSiteIdAndCustodianId);
      } catch (final Throwable e) {
        Logs.error("Unable to load locality: " + this.localityName, e);
      }
    }
    return true;
  }
}
