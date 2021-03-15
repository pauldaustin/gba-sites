package ca.bc.gov.gbasites.load.merge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.process.qa.AbstractTaskByLocalityProcess;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBC;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBC;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Punctual;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.file.Paths;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;
import com.revolsys.util.Debug;
import com.revolsys.util.Strings;

public class SitePointMerger extends AbstractTaskByLocalityProcess
  implements Cancellable, SitePoint {

  public static final String MOVED = "Moved";

  private static final List<String> UPDATE_IGNORE_FIELD_NAMES = Arrays.asList(SITE_ID,
    PARENT_SITE_ID);

  private static final List<String> UPDATE_IGNORE_NULL_FIELD_NAMES = Arrays.asList(POSTAL_CODE,
    SITE_LOCATION_CODE, SITE_NAME_1, SITE_TYPE_CODE, STREET_NAME, STREET_NAME_ALIAS_1_ID);

  private static final List<String> COPY_NULL_FIELD_NAMES = Lists.newArray(CUSTODIAN_SITE_ID,
    POSTAL_CODE);

  private static final List<String> FIELD_NAMES = Lists
    .toArray(ImportSites.getSitePointTsvRecordDefinition().getFieldNames());

  static {
    FIELD_NAMES.removeAll(UPDATE_IGNORE_FIELD_NAMES);
  }

  public static boolean isAddressBc(final Record record) {
    final String organizationName = record.getString(CREATE_PARTNER_ORG);
    return isAddressBc(organizationName);
  }

  public static boolean isAddressBc(final String organizationName) {
    return organizationName.equals(AddressBC.PROVIDER_ICI_SOCIETY);
  }

  private static void mergeUnitDescriptors(final Record record1, final Record record2) {
    final String custodianAddress1 = record1.getString(CUSTODIAN_FULL_ADDRESS);
    final String custodianAddress2 = record2.getString(CUSTODIAN_FULL_ADDRESS);

    final String unitDescriptor1 = record1.getString(UNIT_DESCRIPTOR);
    final String unitDescriptor2 = record2.getString(UNIT_DESCRIPTOR);

    final RangeSet range1 = RangeSet.newRangeSet(unitDescriptor1);
    final RangeSet range2 = RangeSet.newRangeSet(unitDescriptor2);

    final RangeSet newRange = new RangeSet();
    newRange.addRanges(range1);
    newRange.addRanges(range2);
    record1.setValue(UNIT_DESCRIPTOR, newRange.toString());
    String baseAddress = null;
    if (custodianAddress1 == null) {
      if (custodianAddress2 != null) {
        baseAddress = removeUnitFromAddress(custodianAddress2, range2);
      }
    } else if (custodianAddress2 == null) {
      baseAddress = removeUnitFromAddress(custodianAddress1, range1);
    } else {
      baseAddress = retainCommon(custodianAddress1, custodianAddress2);
    }
    if (baseAddress != null) {
      final String custodianAddress = SitePoint.getSimplifiedUnitDescriptor(newRange) + " "
        + baseAddress;
      record1.setValue(CUSTODIAN_FULL_ADDRESS, custodianAddress);
    }
  }

  private static String removeUnitFromAddress(String address, final RangeSet range) {
    if (address == null) {
      return null;
    } else {
      final String unit = SitePoint.getSimplifiedUnitDescriptor(range);
      if (address.startsWith(unit)) {
        address = address.substring(unit.length());
      } else if (address.startsWith(unit.replace('~', '-'))) {
        address = address.substring(unit.length());
      } else {
        final String unit2 = range.toString().replace(',', '-');
        if (address.startsWith(unit2)) {
          address = address.substring(unit2.length());
        } else {
          final String unit3 = range.getFrom() + "-" + range.getTo();
          if (address.startsWith(unit3)) {
            address = address.substring(unit3.length());
          } else {
            final String matchString = "-" + unit + " ";
            final int index = address.indexOf(matchString);
            if (index != -1) {
              address = address.substring(0, index) + " "
                + address.substring(index + matchString.length());
            }
          }
        }
      }
      if (address.startsWith(",")) {
        address = address.substring(1);
      }
      if (address.startsWith("-")) {
        address = address.substring(1);
      }
      if (address.startsWith(" ")) {
        address = address.substring(1);
      }
      return address;
    }
  }

  private static String retainCommon(final String value1, final String value2) {
    final int length1 = value1.length();
    final int length2 = value2.length();
    int minLength = Math.min(length1, length2);
    int endLength = 0;
    for (int i = 0; i < minLength; i++) {
      if (value1.charAt(length1 - i - 1) == value2.charAt(length2 - i - 1)) {
        endLength++;
      }
    }
    minLength -= endLength;
    int startLength = 0;
    for (int i = 0; i < minLength; i++) {
      if (value1.charAt(i) == value2.charAt(i)) {
        startLength++;
      }
    }
    final String prefix = value1.substring(0, startLength).replaceAll("[^\\w\\d]$", "");
    final String suffix = value1.substring(length1 - endLength, length1)
      .replaceAll("^[^\\w\\d]", "");
    return Strings.toString(" ", prefix, suffix);
  }

  private static void setMergedGeometry(final Record site1, final Punctual point1,
    final Punctual point2) {
    final Punctual mergedPoint = point1.union(point2);
    site1.setGeometryValue(mergedPoint);
  }

  public static boolean setMergedPoint(final Record record1, final Record record2) {
    final Punctual point1 = record1.getGeometry();
    final Punctual point2 = record2.getGeometry();
    if (point1.equals(point2)) {
      return false;
    } else {
      setMergedGeometry(record1, point1, point2);
      return true;
    }
  }

  public static void setMergedUnit(final Record record1, final Record record2) {
    mergeUnitDescriptors(record1, record2);
    SitePoint.updateFullAddress(record1);
    setNullFields(record1, record2);
  }

  public static void setNullFields(final Record site1, final Record site2) {
    for (final String fieldName : COPY_NULL_FIELD_NAMES) {
      if (!site1.hasValue(fieldName) && site2.hasValue(fieldName)) {
        site1.setValue(site2, fieldName);
      }
    }
  }

  private final JdbcRecordStore recordStore = GbaSiteDatabase.getRecordStore();

  private final RecordMergeCounters countersAddressBc;

  private final RecordMergeCounters countersProvider;

  private final RecordMergeCounters countersGeoBc;

  private RecordWriter writer;

  private RecordWriter writerDelete;

  private Counter counterInsert;

  private Counter counterUpdate;

  private Counter counterDelete;

  private Counter counterToDelete;

  private Counter counterMatched;

  private Counter counterMergedWrite;

  public SitePointMerger(final ImportSites dialog) {
    super(dialog);
    this.countersProvider = new RecordMergeCounters(
      this.dialog.labelCounts(ImportSites.PROVIDERS), null);
    this.countersGeoBc = new RecordMergeCounters(this.dialog.labelCounts(GeoBC.NAME),
      GeoBC.PARTNER_ORGANIZATION);
    this.countersAddressBc = new RecordMergeCounters(
      this.dialog.labelCounts(AddressBC.NAME), AddressBC.PARTNER_ORGANIZATION);

  }

  private synchronized void deleteRecord(final Record record, final boolean forceDelete) {
    if (forceDelete) {
      this.counterDelete.add();
    } else {
      writeRecord(record);
      this.writerDelete.write(record);
      this.counterToDelete.add();
    }
  }

  private void deleteRecords(final List<Record> records, final boolean forceDelete) {
    for (final Record record : cancellable(records)) {
      deleteRecord(record, forceDelete);
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

  private Counter getCounter(final String countName) {
    return this.dialog.getCounter("Locality", this.localityName, countName);
  }

  @Override
  protected void initLocality(final Identifier localityId) {
    super.initLocality(localityId);
    this.counterInsert = getCounter(BatchUpdateDialog.INSERTED);
    this.counterUpdate = getCounter(BatchUpdateDialog.UPDATED);
    this.counterToDelete = getCounter(ImportSites.TO_DELETE);
    this.counterDelete = getCounter(BatchUpdateDialog.DELETED);
    this.counterMatched = getCounter(ImportSites.MATCHED);

    this.counterMergedWrite = getCounter(ImportSites.MERGED_WRITE);

    this.countersProvider.init(this.localityName);
    this.countersGeoBc.init(this.localityName);
    this.countersAddressBc.init(this.localityName);

  }

  protected void insertRecord(final Record record) {
    writeRecord(record);
    this.counterInsert.add();
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  private RecordsForLocality loadLocalityProviderSitePoints() {
    final RecordsForLocality records = new RecordsForLocality();

    loadLocalityProviderSitePoints(records, null, ProviderSitePointConverter.PROVIDER_DIRECTORY,
      this.countersProvider);
    loadLocalityProviderSitePoints(records, GeoBC.PARTNER_ORGANIZATION, GeoBC.DIRECTORY,
      this.countersGeoBc);
    loadLocalityProviderSitePoints(records, AddressBC.PARTNER_ORGANIZATION, AddressBC.DIRECTORY,
      this.countersAddressBc);

    return records;
  }

  private void loadLocalityProviderSitePoints(final RecordsForLocality records,
    final PartnerOrganization partnerOrganization, final Path directory,
    final RecordMergeCounters counters) {
    final List<Path> files = ImportSites.SITE_POINT_BY_LOCALITY.listLocalityFiles(directory,
      this.localityName);
    for (final Path file : cancellable(files)) {
      records.addRecords(this, file, counters);
    }
  }

  private RecordsForLocality loadSitePoints(final AtomicPathUpdator pathUpdator,
    final boolean targetExists) {
    final RecordsForLocality records = new RecordsForLocality();
    if (targetExists) {
      final Counter counter = getCounter(ImportSites.MERGED_READ);
      final Path file = pathUpdator.getTargetPath();
      records.addRecords(this, file, counter);
    }
    return records;
  }

  private void merge01Locality(final RecordsForLocality gbaLocalitySites,
    final RecordsForLocality providerLocalitySites) {
    for (final String streetName : cancellable(providerLocalitySites.getStreetNames())) {
      final RecordsForStreetName gbaStreetRecords = gbaLocalitySites.removeStreet(streetName);
      final RecordsForStreetName providerStreetRecords = providerLocalitySites
        .getStreet(streetName);
      merge02Street(streetName, gbaStreetRecords, providerStreetRecords);
    }

    gbaLocalitySites.deleteRecords(this.counterDelete);
  }

  private void merge02Street(final String streetName, final RecordsForStreetName gbaStreetRecords,
    final RecordsForStreetName providerStreetRecords) {
    for (final Integer civicNumber : cancellable(providerStreetRecords.getCivicNumbers())) {
      final RecordsForCivicNumber gbaCivicNumberRecords = gbaStreetRecords
        .removeCivicNumber(civicNumber);
      final RecordsForCivicNumber providerCivicNumberRecords = providerStreetRecords
        .getCivicNumber(civicNumber);
      merge03CivicNumber(civicNumber, streetName, gbaCivicNumberRecords,
        providerCivicNumberRecords);
    }

    gbaStreetRecords.deleteRecords(this.counterDelete);
  }

  private void merge03CivicNumber(final Integer civicNumber, final String streetName,
    final RecordsForCivicNumber gbaCivicNumberRecords,
    final RecordsForCivicNumber providerCivicNumberRecords) {

    for (final String civicNumberSuffix : cancellable(
      providerCivicNumberRecords.getCivicNumberSuffixes())) {
      final RecordsForCivicNumberSuffix gbaRecords = gbaCivicNumberRecords
        .removeCivicNumberSuffix(civicNumberSuffix);
      final RecordsForCivicNumberSuffix providerRecords = providerCivicNumberRecords
        .getCivicNumberSuffix(civicNumberSuffix);
      merge04CivicNumberSuffix(civicNumber, civicNumberSuffix, streetName, gbaRecords,
        providerRecords);
    }

    gbaCivicNumberRecords.deleteRecords(this.counterDelete);
  }

  private void merge04CivicNumberSuffix(final Integer civicNumber, final String civicNumberSuffix,
    final String streetName, final RecordsForCivicNumberSuffix gbaRecordsForSuffix,
    final RecordsForCivicNumberSuffix providerRecordsForSuffix) {
    providerRecordsForSuffix.mergeFullAddress(this.countersProvider);
    providerRecordsForSuffix.mergeFullAddress(this.countersGeoBc);
    providerRecordsForSuffix.mergeFullAddress(this.countersAddressBc);

    merge05MatchGbaRecords(gbaRecordsForSuffix, providerRecordsForSuffix);
  }

  private void merge05MatchGbaRecords(final RecordsForCivicNumberSuffix gbaRecordsForSuffix,
    final RecordsForCivicNumberSuffix providerRecordsForSuffix) {
    final List<Record> gbaRecords = gbaRecordsForSuffix.getRecords();
    final List<Record> providerRecords = providerRecordsForSuffix.getRecords();

    for (final Record providerRecord : cancellable(providerRecords)) {
      final Punctual point1 = providerRecord.getGeometry();

      boolean matched = false;
      for (final Iterator<Record> gbaIterator = gbaRecords.iterator(); gbaIterator.hasNext();) {
        final Record gbaRecord = gbaIterator.next();
        final Punctual point2 = gbaRecord.getGeometry();
        if (point1.intersects(point2)) {
          gbaIterator.remove();
          if (matched) {
            this.counterDelete.add();
          } else {
            updateRecord(gbaRecord, providerRecord);
            matched = true;
          }
        }
      }
      if (!matched) {
        for (final Iterator<Record> gbaIterator = gbaRecords.iterator(); gbaIterator.hasNext();) {
          final Record gbaRecord = gbaIterator.next();
          if (gbaRecord.equalValue(providerRecord, FULL_ADDRESS)
            && gbaRecord.equalValue(providerRecord, CREATE_PARTNER_ORG)) {
            gbaIterator.remove();
            if (matched) {
              this.counterDelete.add();
            } else {
              updateRecord(gbaRecord, providerRecord);
              matched = true;
            }
          }
        }
      }
      if (!matched) {
        insertRecord(providerRecord);
      }
    }

    deleteRecords(gbaRecords, false);
  }

  @Override
  public boolean processLocality() {
    Path targetDeleteFile = null;
    try (
      AtomicPathUpdator pathUpdator = ImportSites.SITE_POINT.newLocalityPathUpdator(this.dialog,
        ImportSites.SITES_DIRECTORY, this.localityName);
      AtomicPathUpdator pathUpdatorDelete = ImportSites.SITE_POINT_TO_DELETE
        .newLocalityPathUpdator(this.dialog, ImportSites.SITES_DIRECTORY, this.localityName);
      Transaction transaction = this.recordStore.newTransaction()) {

      final RecordDefinition recordDefinition = ImportSites.getSitePointTsvRecordDefinition();
      final boolean targetExists = pathUpdator.isTargetExists();
      final Path file = pathUpdator.getPath();
      final Path fileDelete = pathUpdatorDelete.getPath();
      try (
        RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file);
        RecordWriter writerDelete = RecordWriter.newRecordWriter(recordDefinition, fileDelete);) {
        this.writer = writer;
        this.writerDelete = writerDelete;
        final RecordsForLocality providerSites = loadLocalityProviderSitePoints();
        final RecordsForLocality sites = loadSitePoints(pathUpdator, targetExists);

        merge01Locality(sites, providerSites);
      }
      targetDeleteFile = pathUpdatorDelete.getTargetPath();
      return true;
    } catch (final Exception e) {
      Logs.error(this, "Error merging sites for: " + e.getMessage(), e);
      return false;
    } finally {
      if (targetDeleteFile != null && this.counterToDelete.get() == 0) {
        try {
          Files.delete(targetDeleteFile);
          final String baseName = Paths.getBaseName(targetDeleteFile);
          final Path prjFile = targetDeleteFile.resolveSibling(baseName + ".prj");
          Files.delete(prjFile);
        } catch (final NoSuchFileException e) {
        } catch (final IOException e) {
          Logs.error(this, e);
        }
      }
    }
  }

  private void updateRecord(final Record gbaRecord, final Record providerRecord) {
    final MapEx newValues = new LinkedHashMapEx();
    for (final String fieldName : FIELD_NAMES) {
      final Object providerValue = providerRecord.getValue(fieldName);
      if (REGIONAL_DISTRICT_NAME.equals(fieldName)) {
        Debug.noOp();
      }
      if (!gbaRecord.equalValue(fieldName, providerValue)) {
        newValues.put(fieldName, providerValue);
      }
    }
    updateRecord(gbaRecord, providerRecord, newValues);
  }

  private synchronized void updateRecord(final Record gbaRecord, final Record providerRecord,
    final MapEx newValues) {
    if (newValues.isEmpty()) {
      this.counterMatched.add();
    } else {
      gbaRecord.setValues(newValues);
      this.counterUpdate.add();
    }
    writeRecord(gbaRecord);
  }

  private void writeRecord(final Record record) {
    this.writer.write(record);
    this.counterMergedWrite.add();
  }
}
