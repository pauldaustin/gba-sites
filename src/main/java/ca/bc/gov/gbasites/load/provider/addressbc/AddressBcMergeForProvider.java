package ca.bc.gov.gbasites.load.provider.addressbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.ArchiveAndChangeLogController;
import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.IntegrationAction;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.index.PointRecordMap;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;

public class AddressBcMergeForProvider implements Cancellable, SitePoint {

  private static final List<String> UPDATE_FIELD_NAMES = Arrays.asList(COMMUNITY_ID, CIVIC_NUMBER,
    CIVIC_NUMBER_RANGE, CIVIC_NUMBER_SUFFIX, EXTENDED_DATA, FEATURE_STATUS_CODE, FULL_ADDRESS,
    LOCALITY_ID, POSTAL_CODE, REGIONAL_DISTRICT_ID, SITE_LOCATION_CODE, SITE_NAME_1, SITE_TYPE_CODE,
    STREET_NAME, STREET_NAME_ID, STREET_NAME_ALIAS_1_ID, UNIT_DESCRIPTOR, GEOMETRY);

  private static final List<String> UPDATE_IGNORE_NULL_FIELD_NAMES = Arrays.asList(POSTAL_CODE,
    SITE_LOCATION_CODE, SITE_NAME_1, SITE_TYPE_CODE, STREET_NAME, STREET_NAME_ALIAS_1_ID);

  private static final Set<String> MOVED_FIELD_NAMES = Sets.newLinkedHash(GEOMETRY);

  private static final Set<String> UNIT_DESCRIPTOR_FIELD_NAMES = Sets.newLinkedHash(FULL_ADDRESS,
    CUSTODIAN_FULL_ADDRESS, UNIT_DESCRIPTOR);

  private static final Set<String> POSTAL_CODE_FIELD_NAMES = Sets.newLinkedHash(POSTAL_CODE);

  private final PartnerOrganization partnerOrganization;

  private final Path siteFile;

  private final JdbcRecordStore recordStore = GbaController.getGbaRecordStore();

  private final AddressBcImportSites importSites;

  private final LabelCountMapTableModel counts;

  private final ArchiveAndChangeLogController archiveAndChangeLog;

  private final RecordWriter changedRecordWriter;

  private int changeCount = 1000000;

  public AddressBcMergeForProvider(final AddressBcMerge addressBcMerge,
    final AddressBcImportSites importSites, final PartnerOrganization partnerOrganization,
    final Path siteFile) {
    this.importSites = importSites;
    this.archiveAndChangeLog = this.importSites.getArchiveAndChangeLog();
    this.partnerOrganization = partnerOrganization;
    this.siteFile = siteFile;
    this.counts = importSites.getLabelCountTableModel("Merge");
    this.changedRecordWriter = addressBcMerge.changedRecordWriter;
  }

  private synchronized void deleteSite(final Record site, final boolean forceDelete) {
    if (this.importSites.action4UpdateDb) {
      // archiveAndChangeLog.deleteRecord(site);
    }
    writeChangeRecord(site, "Delete", null);

    this.counts.addCount(this.partnerOrganization, "Delete");
  }

  private void deleteSites(final List<Record> deleteSites, final boolean forceDelete) {
    for (final Record site : deleteSites) {
      deleteSite(site, forceDelete);
    }
  }

  private void deleteSitesByCivicNumber(
    final Map<Integer, Map<String, List<Record>>> byCivicNumber) {
    for (final Map<String, List<Record>> byCivicNumberSuffix : byCivicNumber.values()) {
      deleteSitesByCivicNumberSuffix(byCivicNumberSuffix);
    }
  }

  private void deleteSitesByCivicNumberSuffix(final Map<String, List<Record>> byCivicNumberSuffix) {
    for (final List<Record> sites : byCivicNumberSuffix.values()) {
      deleteSites(sites, false);
    }
  }

  private void deleteSitesByStreetName(
    final Map<String, Map<Integer, Map<String, List<Record>>>> byStreetName) {
    for (final Map<Integer, Map<String, List<Record>>> byCivicNumber : byStreetName.values()) {
      deleteSitesByCivicNumber(byCivicNumber);
    }
  }

  public void deleteTempFiles(final Path directory) {
    try {
      Files.list(directory).forEach(path -> {
        final String fileName = Paths.getFileName(path);
        if (fileName.startsWith("_")) {
          if (!Paths.deleteDirectories(path)) {
            Logs.error(this, "Unable to remove temporary files from: " + path);
          }
        }
      });
    } catch (final IOException e) {
      Logs.error(this, "Unable to remove temporary files from: " + directory, e);
    }
  }

  private void insertSite(final Record providerSite, final boolean writeChangeLog) {
    final Record site = this.recordStore.newRecord(SiteTables.SITE_POINT);
    site.setValue(CUSTODIAN_SESSION_ID, this.archiveAndChangeLog.getIntegrationSessionId());
    site.setValuesNotNull(providerSite);
    if (this.importSites.action4UpdateDb) {
      this.archiveAndChangeLog.insertRecord(site);
    }
    if (writeChangeLog) {
      writeChangeRecord(site, BatchUpdateDialog.INSERTED, null);
    }
    this.counts.addCount(this.partnerOrganization, BatchUpdateDialog.INSERTED);
  }

  private void insertSitesAll(final List<Record> sites, final boolean writeChangeLog) {
    for (final Record site : sites) {
      insertSite(site, writeChangeLog);
    }
  }

  private void insertSitesAll(final Map<Identifier, List<Record>> providerSitesByLocality,
    final boolean writeChangeLog) {
    for (final List<Record> sites : providerSitesByLocality.values()) {
      insertSitesAll(sites, writeChangeLog);
    }
  }

  private void insertSitesByCivicNumber(
    final Map<Integer, Map<String, List<Record>>> byCivicNumber) {
    for (final Map<String, List<Record>> bySuffix : byCivicNumber.values()) {
      insertSitesByCivicNumberSuffix(bySuffix);
    }
  }

  private void insertSitesByCivicNumberSuffix(final Map<String, List<Record>> bySuffix) {
    for (final List<Record> sites : bySuffix.values()) {
      insertSitesAll(sites, true);
    }
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  private <K> Iterable<K> keys(final Map<K, ?> map) {
    return cancellable(Lists.toArray(map.keySet()));
  }

  private Map<Identifier, List<Record>> loadGbaSites(
    final PartnerOrganization partnerOrganization) {
    final Identifier partnerOrganizationId = partnerOrganization.getPartnerOrganizationId();
    final Query query = new Query(SiteTables.SITE_POINT, Q.and(//
      Q.equal(CUSTODIAN_PARTNER_ORG_ID, partnerOrganizationId) //
    ))//
      .addOrderBy(LOCALITY_ID) //
      .addOrderBy(STREET_NAME) //
      .addOrderBy(CIVIC_NUMBER) //
      .addOrderBy(CIVIC_NUMBER_SUFFIX) //
      .addOrderBy(UNIT_DESCRIPTOR) //
    ;
    try (
      RecordReader reader = this.recordStore.getRecords(query)) {
      return splitByLocality(AddressBcMerge.GBA_READ, reader);
    }
  }

  private Map<Identifier, List<Record>> loadProviderSites(final Path siteFile) {
    final Map<Identifier, List<Record>> providerSitesByLocality;
    try (
      RecordReader fileReader = RecordReader.newRecordReader(siteFile)) {
      providerSitesByLocality = splitByLocality(AddressBcMerge.PROVIDER_READ, fileReader);
    }
    return providerSitesByLocality;
  }

  private void matchByPoint(final List<Record> gbaSites, final List<Record> providerSites) {
    final PointRecordMap gbaByPoint = new PointRecordMap(gbaSites);
    final PointRecordMap providerByPoint = new PointRecordMap(providerSites);
    for (final Point point : providerByPoint.getKeys()) {
      final List<Record> gbaRecords = gbaByPoint.getRecords(point);
      final List<Record> providerRecords = providerByPoint.getRecords(point);
      if (gbaRecords.size() == 1 && providerRecords.size() == 1) {
        final Record gbaSite = gbaRecords.get(0);
        final Record providerSite = providerRecords.get(0);
        updateSite(gbaSite, providerSite);
        gbaSites.remove(gbaSite);
        providerSites.remove(providerSite);
      } else if (!gbaRecords.isEmpty()) {
        Logs.error(this, "Should not be multiple sites with the same point");
      }
    }
  }

  private void matchByUnitDescriptor(final List<Record> gbaSites,
    final List<Record> providerSites) {
    final Map<String, List<Record>> gbaByUnit = splitByUnitDescriptor(gbaSites);
    final Map<String, List<Record>> providerByUnit = splitByUnitDescriptor(providerSites);

    for (final String unitDescriptor : keys(providerByUnit)) {
      final List<Record> gbaRecords = gbaByUnit.getOrDefault(unitDescriptor,
        Collections.emptyList());
      final List<Record> providerRecords = providerByUnit.getOrDefault(unitDescriptor,
        Collections.emptyList());
      if (!gbaRecords.isEmpty()) {
        final int count = Math.min(providerRecords.size(), gbaRecords.size());
        for (int i = 0; i < count; i++) {
          final Record gbaSite = gbaRecords.get(i);
          final Point gbaPoint = gbaSite.getGeometry();
          Record closestProviderSite = null;
          double closestDistance = Double.MAX_VALUE;
          for (final Record providerSite : providerRecords) {
            final Point providerPoint = providerSite.getGeometry();
            final double distance = gbaPoint.distance(providerPoint);
            if (distance < closestDistance) {
              closestDistance = distance;
              closestProviderSite = providerSite;
            }
          }
          updateSite(gbaSite, closestProviderSite);
          providerRecords.remove(closestProviderSite);
          gbaSites.remove(gbaSite);
          providerSites.remove(closestProviderSite);
        }
      }
    }
  }

  private void merge01Locality(final List<Record> gbaLocalitySites,
    final List<Record> providerLocalitySites) {
    if (Property.isEmpty(gbaLocalitySites)) {
      insertSitesAll(providerLocalitySites, true);
    } else {
      final Map<String, Map<Integer, Map<String, List<Record>>>> gbaSitesByStreetName = splitByStreetNumberAndSuffix(
        gbaLocalitySites);
      final Map<String, Map<Integer, Map<String, List<Record>>>> providerSitesByStreetName = splitByStreetNumberAndSuffix(
        providerLocalitySites);
      for (final String streetName : keys(providerSitesByStreetName)) {
        final Map<Integer, Map<String, List<Record>>> gbaByCivicNumber = gbaSitesByStreetName
          .remove(streetName);
        final Map<Integer, Map<String, List<Record>>> providerByCivicNumber = providerSitesByStreetName
          .remove(streetName);
        merge02Street(streetName, gbaByCivicNumber, providerByCivicNumber);

      }

      // Delete any GBA sites not matched
      deleteSitesByStreetName(gbaSitesByStreetName);
    }
  }

  private void merge02Street(final String streetName,
    final Map<Integer, Map<String, List<Record>>> gbaByCivicNumber,
    final Map<Integer, Map<String, List<Record>>> providerByCivicNumber) {
    if (Property.isEmpty(gbaByCivicNumber)) {
      insertSitesByCivicNumber(providerByCivicNumber);
    } else {
      for (final Integer civicNumber : keys(providerByCivicNumber)) {
        final Map<String, List<Record>> gbaByCivicNumberSuffix = gbaByCivicNumber
          .remove(civicNumber);
        final Map<String, List<Record>> providerByCivicNumberSuffix = providerByCivicNumber
          .remove(civicNumber);
        merge03CivicNumber(civicNumber, streetName, gbaByCivicNumberSuffix,
          providerByCivicNumberSuffix);
      }

      // Delete any GBA sites not matched
      deleteSitesByCivicNumber(gbaByCivicNumber);
    }
  }

  private void merge03CivicNumber(final Integer civicNumber, final String streetName,
    final Map<String, List<Record>> gbaByCivicNumberSuffix,
    final Map<String, List<Record>> providerByCivicNumberSuffix) {
    if (Property.isEmpty(gbaByCivicNumberSuffix)) {
      insertSitesByCivicNumberSuffix(providerByCivicNumberSuffix);
    } else {
      for (final String civicNumberSuffix : keys(providerByCivicNumberSuffix)) {
        final List<Record> gbaSites = gbaByCivicNumberSuffix.remove(civicNumberSuffix);
        final List<Record> providerSites = providerByCivicNumberSuffix.remove(civicNumberSuffix);
        merge04CivicNumberSuffix(civicNumber, civicNumberSuffix, streetName, gbaSites,
          providerSites);
      }

      // Delete any GBA sites not matched
      deleteSitesByCivicNumberSuffix(gbaByCivicNumberSuffix);
    }
  }

  private void merge04CivicNumberSuffix(final Integer civicNumber, final String civicNumberSuffix,
    final String streetName, final List<Record> gbaSites, final List<Record> providerSites) {
    if (Property.isEmpty(gbaSites)) {
      insertSitesAll(providerSites, true);
    } else {
      if (civicNumber == 5867 && streetName.equals("129 St")) {
        Debug.noOp();
      }
      matchByPoint(gbaSites, providerSites);
      matchByUnitDescriptor(gbaSites, providerSites);
      // matchSites(gbaSites, providerSites, this::matchFullAddressPoint);
      if (gbaSites.size() == 1 && providerSites.size() == 1) {
        final Record gbaSite = gbaSites.get(0);
        final Record providerSite = providerSites.get(0);
        updateSite(gbaSite, providerSite);
      } else if (gbaSites.isEmpty()) {
        insertSitesAll(providerSites, true);
      } else {
        deleteSites(gbaSites, false);
        if (!providerSites.isEmpty()) {
          insertSitesAll(providerSites, true);
        }
      }
    }
  }

  public void run() {
    try (
      Transaction transaction = this.recordStore.newTransaction(Propagation.REQUIRES_NEW)) {
      final Map<Identifier, List<Record>> providerSitesByLocality = loadProviderSites(
        this.siteFile);
      final Map<Identifier, List<Record>> gbaSitesByLocality = loadGbaSites(
        this.partnerOrganization);
      if (gbaSitesByLocality.isEmpty()) {
        insertSitesAll(providerSitesByLocality, false);
      } else {
        for (final Identifier localityId : keys(providerSitesByLocality)) {
          final List<Record> gbaLocalitySites = gbaSitesByLocality.remove(localityId);
          final List<Record> providerLocalitySites = providerSitesByLocality.remove(localityId);
          merge01Locality(gbaLocalitySites, providerLocalitySites);
        }

        // Delete any in localities that aren't in the input data
        for (final List<Record> sites : gbaSitesByLocality.values()) {
          deleteSites(sites, false);
        }

      }
    } catch (final Exception e) {
      Logs.error(this, "Error merging sites for: " + e);
    }

  }

  private Map<Identifier, List<Record>> splitByLocality(final String label,
    final RecordReader reader) {
    final Comparator<Identifier> comparator = GbaController.getLocalities().getIdNameComparator();
    final Map<Identifier, List<Record>> sitesByLocality = new TreeMap<>(comparator);
    for (final Record record : reader) {
      final Identifier localityId = record.getIdentifier(LOCALITY_ID, DataTypes.INT);
      Maps.addToList(sitesByLocality, localityId, record);
      this.counts.addCount(this.partnerOrganization, label);
    }
    return sitesByLocality;
  }

  private Map<String, Map<Integer, Map<String, List<Record>>>> splitByStreetNumberAndSuffix(
    final Iterable<Record> sites) {
    final Map<String, Map<Integer, Map<String, List<Record>>>> sitesByStreet = new TreeMap<>();
    for (final Record site : cancellable(sites)) {
      final String streeName = site.getString(STREET_NAME);
      final Integer civicNumber = site.getInteger(CIVIC_NUMBER);
      final String civicNumberSuffix = site.getString(CIVIC_NUMBER_SUFFIX, "");

      final Map<Integer, Map<String, List<Record>>> sitesByCivicNumber = Maps.get(sitesByStreet,
        streeName, Maps.factoryTree());
      final Map<String, List<Record>> sitesBySuffix = Maps.get(sitesByCivicNumber, civicNumber,
        Maps.factoryTree());
      Maps.addToList(sitesBySuffix, civicNumberSuffix, site);
    }
    return sitesByStreet;
  }

  private Map<String, List<Record>> splitByUnitDescriptor(final List<Record> gbaSites) {
    final Map<String, List<Record>> gbaByPoint = new TreeMap<>();
    for (final Record site : gbaSites) {
      final String unitDescriptor = site.getString(UNIT_DESCRIPTOR, "");
      Maps.addToList(gbaByPoint, unitDescriptor, site);
    }
    return gbaByPoint;
  }

  private void updateSite(final Record gbaSite, final Record providerSite) {
    final MapEx newValues = new LinkedHashMapEx();
    if (!gbaSite.equalValue(providerSite, UNIT_DESCRIPTOR)) {
      final RangeSet gbaRange = SitePoint.getUnitDescriptorRanges(gbaSite);
      final RangeSet providerRange = SitePoint.getUnitDescriptorRanges(providerSite);
      providerRange.addRanges(gbaRange);
      providerSite.setValue(UNIT_DESCRIPTOR, providerRange.toString());
      SitePoint.updateFullAddress(providerSite);
    }
    for (final String fieldName : UPDATE_FIELD_NAMES) {
      final Object providerValue = providerSite.getValue(fieldName);
      if (!gbaSite.equalValue(fieldName, providerValue)) {
        if (providerValue != null || !UPDATE_IGNORE_NULL_FIELD_NAMES.contains(fieldName)) {
          newValues.put(fieldName, providerValue);
        }
      }
    }
    updateSite(gbaSite, providerSite, newValues);
  }

  private synchronized void updateSite(final Record gbaSite, final Record providerSite,
    final MapEx newValues) {
    if (newValues.isEmpty()) {
      this.counts.addCount(this.partnerOrganization, AddressBcMerge.MATCHED);
    } else {
      String action = BatchUpdateDialog.UPDATED;
      synchronized (this.changedRecordWriter) {
        final Set<String> changedFieldNames = newValues.keySet();

        boolean logChanges = true;
        if (MOVED_FIELD_NAMES.equals(changedFieldNames)) {
          action = AddressBcMerge.MOVED;
          logChanges = false;
        } else if (Collections.singleton(UNIT_DESCRIPTOR).equals(changedFieldNames)) {
          if (newValues.hasValue(UNIT_DESCRIPTOR)) {
            logChanges = false;
          }
        } else if (UNIT_DESCRIPTOR_FIELD_NAMES.equals(changedFieldNames)) {
          if (newValues.hasValue(UNIT_DESCRIPTOR)) {
            logChanges = false;
          }
        } else if (POSTAL_CODE_FIELD_NAMES.equals(changedFieldNames)) {
          if (newValues.hasValue(POSTAL_CODE)) {
            logChanges = false;
          }
        }
        if (logChanges) {
          final String changedValuesString = changedFieldNames.toString();
          writeChangeRecord(gbaSite, action, changedValuesString);
          writeChangeRecord(providerSite, action, changedValuesString);
        }
        newValues.put(CUSTODIAN_SESSION_ID, this.archiveAndChangeLog.getIntegrationSessionId());
        if (this.importSites.action4UpdateDb) {
          this.archiveAndChangeLog.updateRecord(gbaSite, newValues,
            IntegrationAction.UPDATE_FIELD_AND_GEOMETRY);
        } else {
          gbaSite.setValues(newValues);
        }
      }
      this.counts.addCount(this.partnerOrganization, action);
    }
  }

  private void writeChangeRecord(final Record site, final String changeType,
    final String changeValues) {
    synchronized (this.changedRecordWriter) {
      final Record changeRecord = this.changedRecordWriter.newRecord(site);
      changeRecord.setValue("CHANGE_INDEX", ++this.changeCount);
      changeRecord.setValue("PARTNER_ORGANIZATION", this.partnerOrganization);
      changeRecord.setValue("CHANGE_TYPE", changeType);
      changeRecord.setValue("CHANGE_VALUES", changeValues);
      changeRecord.setGeometryValue(site);
      this.changedRecordWriter.write(changeRecord);
    }
  }

}
