package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.itn.model.GbaItnTables;
import ca.bc.gov.gba.itn.model.GbaType;
import ca.bc.gov.gba.itn.model.TransportLine;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.Localities;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.graph.linemerge.LineMerger;
import com.revolsys.geometry.index.RecordSpatialIndex;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Lineal;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.simplify.TopologyPreservingSimplifier;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class GeocoderCaSiteConverter extends AbstractSiteConverter implements Cancellable {

  private static final BoundaryCache LOCALITIES = GbaController.getLocalities();

  private static final Comparator<SitePointProviderRecord> SUFFIX_UNIT_COMPARATOR = (a, b) -> {
    final String suffix1 = a.getString(CIVIC_NUMBER_SUFFIX, "");
    final String suffix2 = b.getString(CIVIC_NUMBER_SUFFIX, "");
    int compare = suffix1.compareTo(suffix2);
    if (compare == 0) {
      final RangeSet descriptor1 = a.getUnitDescriptorRanges();
      final RangeSet descriptor2 = b.getUnitDescriptorRanges();
      compare = descriptor1.compareTo(descriptor2);
      if (compare == 0) {
        final Point point1 = a.getGeometry();
        final Point point2 = b.getGeometry();
        compare = point1.compareTo(point2);
      }
    }
    return compare;
  };

  public static final StructuredNames STRUCTURED_NAMES = GbaController.getStructuredNames();

  public static String getCleanStringIntern(final Record record, final String fieldName) {
    String value = record.getString(fieldName);
    value = Strings.cleanWhitespace(value);
    if (value != null) {
      value = value.toUpperCase();
      value = value.intern();
    }
    return value;
  }

  private final RecordLog allErrorLog;

  private final RecordLog allIgnoreLog;

  private final RecordLog allWarningLog;

  private final GeocoderCaSiteConvert convertProcess;

  private final LabelCountMapTableModel counts;

  private final GeocoderCaImportSites importSites;

  private final Path inputFile;

  private final String localityFileName;

  Consumer<GeocoderCaSite> localityFixes = r -> {
  };

  private final Identifier localityId;

  private String localityName;

  public final String localityNameUpper;

  private final PartnerOrganization partnerOrganization;

  private RecordWriter sitePointWriter;

  private Record sourceRecord;

  public Set<Identifier> structuredNameIds;

  private final Set<String> unmatchedNames = new TreeSet<>();

  public final RecordSpatialIndex<Record> lineNameIndex = RecordSpatialIndex
    .quadTree(Gba.GEOMETRY_FACTORY_2D_1M);

  public GeocoderCaSiteConverter(final GeocoderCaSiteConvert convertProcess,
    final GeocoderCaImportSites importSites, final Path path, final RecordLog allErrorLog,
    final RecordLog allWarningLog, final RecordLog allIgnoreLog) {
    this.importSites = importSites;
    this.convertProcess = convertProcess;
    this.inputFile = path;
    this.counts = convertProcess.counts;
    final String fileName = Paths.getBaseName(path);
    this.localityFileName = fileName.replace("_GEOCODER_CA", "");
    this.localityId = Localities.LOCALITY_ID_BY_FILE_NAME.get(this.localityFileName);
    this.localityName = Localities.LOCALITY_NAME_BY_FILE_NAME.get(this.localityFileName);
    this.localityNameUpper = this.localityName.toUpperCase();
    this.partnerOrganization = convertProcess.getPartnerOrganization();

    this.allErrorLog = allErrorLog;
    this.allWarningLog = allWarningLog;
    this.allIgnoreLog = allIgnoreLog;
    this.ignoreNames.clear();

    loadStructuredNames();
    if ("105 Mile House".equals(this.localityName) || "108 Mile Ranch".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fix108Mile;
    } else if ("Abbotsford".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixAbbotsford;
    } else if ("Burnaby".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixBurnaby;
    } else if ("Chilliwack".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixChilliwack;
    } else if ("Mission".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixMission;
    } else if ("Parksville".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixParksville;
    } else if ("West Vancouver".equals(this.localityName)) {
      this.localityFixes = GeocoderCaSite::fixWestVancouver;
    }
  }

  @Override
  public void addError(final Record record, final String message) {
    this.counts.addCount(this.localityName, BatchUpdateDialog.ERROR);
    this.importSites.addLabelCount(BatchUpdateDialog.ERROR, message, BatchUpdateDialog.ERROR);
    final Point point = record.getGeometry();

    this.allErrorLog.error(this.localityName, message, record, point);
  }

  @Override
  public void addIgnore(final Record record, final String message) {
    super.addIgnore(record, message);
    this.counts.addCount(this.localityName, "Ignored");
    final Point point = record.getGeometry();

    this.allIgnoreLog.error(this.localityName, message, record, point);
  }

  @Override
  public void addWarning(final Record record, final String message) {
    this.counts.addCount(this.localityName, ProviderSitePointConverter.WARNING);
    this.importSites.addLabelCount(ProviderSitePointConverter.WARNING, message,
      ProviderSitePointConverter.WARNING);
    final Point point = record.getGeometry();
    this.allWarningLog.error(this.localityName, message, record, point);
  }

  @Override
  public void addWarning(final String message) {
    addWarning(this.sourceRecord, message);
  }

  @Override
  public SitePointProviderRecord convertRecordSite(final Record sourceRecord, final Point point) {
    this.sourceRecord = sourceRecord;
    final GeocoderCaSite sourceSite = new GeocoderCaSite(this, sourceRecord, point);
    if (!sourceSite.isPartsEqual()) {
      sourceSite.addError("[STREET] does not match parts");
      return null;
    } else if (sourceSite.fixSourceSite()) {
      return newSitePoint(sourceSite);
    } else {
      return null;
    }
  }

  private SitePointProviderRecord convertSite(final Record sourceRecord) {
    final Point point = sourceRecord.getGeometry();
    if (Property.isEmpty(point)) {
      addError(sourceRecord, "Record does not contain a point geometry");
    } else {
      try {
        final Identifier localityId = LOCALITIES.getBoundaryId(point);
        setLocalityName(LOCALITIES.getValue(localityId));
        final SitePointProviderRecord sitePoint = convertRecordSite(sourceRecord, point);
        if (sitePoint == null) {
          throw IgnoreSiteException.error("Converter returned null");
        } else {
          return sitePoint;
        }
      } catch (final NullPointerException e) {
        Logs.error(ImportSites.class, "Null pointer", e);
        addIgnore(sourceRecord, "Null Pointer");
      } catch (final IgnoreSiteException e) {
        addIgnore(sourceRecord, e.getMessage());
      } catch (final Throwable e) {
        Logs.error(this, e);
        addIgnore(sourceRecord, e.getMessage());
      }
    }
    return null;
  }

  @Override
  protected StatisticsDialog getDialog() {
    return this.importSites;
  }

  @Override
  public Identifier getPartnerOrganizationId() {
    return this.partnerOrganization.getPartnerOrganizationId();
  }

  @Override
  public String getPartnerOrganizationShortName() {
    return this.partnerOrganization.getPartnerOrganizationShortName();
  }

  @Override
  public boolean isCancelled() {
    return this.convertProcess.isCancelled();
  }

  private void loadStructuredNames() {
    final Query query = new Query(GbaItnTables.TRANSPORT_LINE) //
      .setDistinct(true)//
      .setFieldNames(TransportLine.STRUCTURED_NAME_1_ID, GbaType.GEOMETRY) //
      .setWhereCondition(Q.and(//
        Q.equal(TransportLine.LEFT_LOCALITY_ID, this.localityId), //
        Q.equal(TransportLine.RIGHT_LOCALITY_ID, this.localityId) //
      ));
    final Map<Identifier, List<LineString>> lineByNameId = new HashMap<>();
    try (
      RecordReader reader = GbaSiteDatabase.getRecordStore().getRecords(query)) {
      for (final Record record : cancellable(reader)) {
        final LineString line = record.getGeometry().convertGeometry(Gba.GEOMETRY_FACTORY_2D_1M);
        final Identifier structuredNameId = record
          .getIdentifier(TransportLine.STRUCTURED_NAME_1_ID);
        Maps.addToList(lineByNameId, structuredNameId, line);
      }
    }
    final RecordDefinition recordDefinition = new RecordDefinitionBuilder()//
      .addField("ID", DataTypes.IDENTIFIER) //
      .addField("NAME", DataTypes.STRING) //
      .addField(GeometryDataTypes.MULTI_LINE_STRING) //
      .getRecordDefinition();
    this.structuredNameIds = Sets.newHash(lineByNameId.keySet());
    for (final Entry<Identifier, List<LineString>> entry : cancellable(lineByNameId.entrySet())) {
      final Identifier id = entry.getKey();
      final List<LineString> lines = entry.getValue();
      Lineal lineal = Gba.GEOMETRY_FACTORY_2D_1M.lineal(LineMerger.merge(lines));
      lineal = (Lineal)TopologyPreservingSimplifier.simplify(lineal, 1);
      final String name = STRUCTURED_NAMES.getValue(id);
      final Record record = recordDefinition.newRecord();
      record.setValue("ID", id);
      record.setValue("NAME", name);
      record.setGeometryValue(lineal);
      this.lineNameIndex.addRecord(record);
    }
  }

  private void mergeDuplicates(final List<SitePointProviderRecord> sites) {
    final int recordCount = sites.size();
    if (recordCount > 1) {
      for (int i = 0; i < sites.size() - 1; i++) {
        final SitePointProviderRecord site1 = sites.get(i);
        for (int j = sites.size() - 1; j > i; j--) {
          String custodianAddress1 = site1.getString(CUSTODIAN_FULL_ADDRESS);
          final Point point1 = site1.getGeometry();
          final String suffix1 = site1.getString(CIVIC_NUMBER_SUFFIX, "");
          final RangeSet range1 = site1.getUnitDescriptorRanges();

          final SitePointProviderRecord site2 = sites.get(j);
          String custodianAddress2 = site2.getString(CUSTODIAN_FULL_ADDRESS);
          final Point point2 = site2.getGeometry();
          final String suffix2 = site2.getString(CIVIC_NUMBER_SUFFIX, "");
          final RangeSet range2 = site2.getUnitDescriptorRanges();
          if (point1.isWithinDistance(point2, 1)) {
            if (site1.equalValuesExclude(site2,
              Arrays.asList(CUSTODIAN_FULL_ADDRESS, GEOMETRY, POSTAL_CODE))) {
              sites.remove(j);
              addWarning(site1, "Duplicate");
              addWarning(site2, "Duplicate");
              if (!site1.hasValue(POSTAL_CODE)) {
                site1.setValue(site2, POSTAL_CODE);
              }
              this.counts.addCount(this.localityName, "Duplicate");
            } else {
              boolean mergeSites = false;
              if (suffix1.equals(suffix2)) {
                if (range1.equals(range2)) {
                  Debug.noOp();
                } else if (range1.isEmpty()) {
                  if (custodianAddress2.endsWith(custodianAddress1)) {
                    site1.setValue(CUSTODIAN_FULL_ADDRESS, custodianAddress2);
                  } else {
                    addError(site1, "Merge CUSTODIAN_FULL_ADDRESS different");
                    addError(site2, "Merge CUSTODIAN_FULL_ADDRESS different");
                  }
                  site1.setUnitDescriptor(range2);
                  mergeSites = true;
                } else {
                  final RangeSet newRange = new RangeSet();
                  newRange.addRanges(range1);
                  newRange.addRanges(range2);
                  site1.setUnitDescriptor(newRange);
                  custodianAddress1 = removeUnitFromAddress(custodianAddress1, range1);
                  custodianAddress2 = removeUnitFromAddress(custodianAddress2, range2);
                  if (custodianAddress1.equals(custodianAddress2)) {
                    final String custodianAddress = SitePoint.getSimplifiedUnitDescriptor(newRange)
                      + " " + custodianAddress1;
                    site1.setValue(CUSTODIAN_FULL_ADDRESS, custodianAddress);
                  } else {
                    addError(site1, "Merge CUSTODIAN_FULL_ADDRESS different");
                    addError(site2, "Merge CUSTODIAN_FULL_ADDRESS different");
                  }
                  mergeSites = true;
                }
              } else {
                Debug.noOp();
              }
              if (mergeSites) {
                SitePoint.updateFullAddress(site1);
                sites.remove(j);
                addWarning(site1, "Merged UNIT_DESCRIPTOR");
                addWarning(site2, "Merged UNIT_DESCRIPTOR");
                if (!site1.hasValue(POSTAL_CODE)) {
                  site1.setValue(site2, POSTAL_CODE);
                }
                this.counts.addCount(this.localityName, "Merged UD");
              }
            }
          }
        }
      }
    }
  }

  public SitePointProviderRecord newSitePoint(final GeocoderCaSite sourceSite) {
    final Point point = sourceSite.getGeometry();
    final SitePointProviderRecord sitePoint = newSitePoint(this, point);
    final String structuredName = sourceSite.getStructuredName();
    if (structuredName == null) {
      addIgnore(sourceSite, "[STREET_NAME] null");
      return null;
    } else {
      final Identifier structuredNameId = sourceSite.getStructuredNameId();
      if (structuredNameId == null) {
        addIgnore(sourceSite, "[STREET_NAME] not found");
        this.unmatchedNames.add(structuredName);
        return null;
      }
      sitePoint.setValue(UNIT_DESCRIPTOR, sourceSite.getUnitDescriptor());
      sitePoint.setValue(CIVIC_NUMBER, sourceSite.getCivicNumber());
      sitePoint.setValue(STREET_NAME, structuredName);
      sitePoint.setValue(STREET_NAME_ID, structuredNameId);

      sitePoint.setValue(POSTAL_CODE, sourceSite.getPostalCode());

      sitePoint.updateFullAddress();

      final String custodianFullAddress = sourceSite.getOriginalFullAddress();
      sitePoint.setValue(CUSTODIAN_FULL_ADDRESS, custodianFullAddress);

      sitePoint.setValue(OPEN_DATA_IND, "Y");
      sitePoint.setValue(LOCALITY_ID, this.localityId);

      final String partnerOrganizationName = getPartnerOrganizationName();
      sitePoint.setCreateModifyOrg(this.createModifyPartnerOrganization);
      sitePoint.setCustodianOrg(this.partnerOrganization);

      return sitePoint;
    }
  }

  private String removeUnitFromAddress(String address, final RangeSet range) {
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
        }
      }
    }
    if (address.startsWith("-")) {
      address = address.substring(1);
    } else if (address.startsWith(" ")) {
      address = address.substring(1);
    }
    return address;
  }

  public void run() {

    final RecordDefinitionImpl siteRecordDefinition = ImportSites.getSitePointTsvRecordDefinition();
    final Path file = this.importSites.getSitePointByProviderDirectory() //
      .resolve(this.localityFileName + "_SITE_POINT_GEOCODER_CA.tsv");
    try (
      RecordReader reader = RecordReader.newRecordReader(this.inputFile);
      RecordWriter sitePointWriter = this.sitePointWriter = RecordWriter
        .newRecordWriter(siteRecordDefinition, file);) {
      this.sitePointWriter.setProperty("useQuotes", false);

      final Map<String, Map<Integer, List<SitePointProviderRecord>>> sitesByStreetAddress = new TreeMap<>();

      final RecordDefinitionImpl recordDefinition = (RecordDefinitionImpl)reader
        .getRecordDefinition();
      recordDefinition.addField("FULL_ADDRESS", DataTypes.STRING);
      recordDefinition.addField("FULL_ADDRESS_PARTS", DataTypes.STRING);

      for (final Record sourceRecord : cancellable(reader)) {
        this.counts.addCount(this.localityName, "Read");
        final SitePointProviderRecord siteRecord = convertSite(sourceRecord);
        if (siteRecord != null) {
          final String streetName = siteRecord.getStructuredName();
          final Integer civicNumber = siteRecord.getCivicNumber();
          Maps.addToList(Maps.factoryTree(), sitesByStreetAddress, streetName, civicNumber,
            siteRecord);
        }
      }
      for (final Map<Integer, List<SitePointProviderRecord>> sitesByCivicNumber : cancellable(
        sitesByStreetAddress.values())) {
        for (final List<SitePointProviderRecord> sites : cancellable(sitesByCivicNumber.values())) {
          sites.sort(SUFFIX_UNIT_COMPARATOR);
          mergeDuplicates(sites);
          for (final Record record : sites) {
            writeSitePoint(record);
          }
        }
      }
      synchronized (getClass()) {
        for (final String name : this.unmatchedNames) {
          System.out.println(this.localityName + "\t" + name);

        }
      }
    } catch (final Throwable e) {
      Logs.error(this, e);
    }
  }

  public void setLocalityName(final String localityName) {
    this.localityName = localityName;
  }

  @Override
  public String toString() {
    return this.localityName;
  }

  public void writeSitePoint(final Record record) {
    final Record writeRecord = this.sitePointWriter.newRecord(record);
    writeRecord.setGeometryValue(record);
    this.sitePointWriter.write(writeRecord);
    this.counts.addCount(this.localityName, "Write");
  }
}
