package ca.bc.gov.gbasites.load.provider.addressbc;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;
import org.jeometry.common.number.Integers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizations;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ImportSites;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.common.converter.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class AddressBcSiteConverter extends AbstractSiteConverter implements Cancellable {

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

  public static String getCleanStringIntern(final Record record, final String fieldName) {
    String value = record.getString(fieldName);
    value = Strings.cleanWhitespace(value);
    if (value != null) {
      value = value.toUpperCase();
      value = value.intern();
    }
    return value;
  }

  private final AddressBcImportSites importSites;

  private final LabelCountMapTableModel counts;

  private final Map<String, Consumer<AddressBcSite>> fixesByProvider = new HashMap<>();

  private final Path inputFile;

  private String localityName;

  private final PartnerOrganization partnerOrganization;

  private RecordWriter sitePointWriter;

  private Record sourceRecord;

  private final AddressBcConvert convertProcess;

  private final RecordLog allErrorLog;

  private final RecordLog allWarningLog;

  public AddressBcSiteConverter(final AddressBcConvert convertProcess,
    final AddressBcImportSites importSites, final Path path, final RecordLog allErrorLog,
    final RecordLog allWarningLog) {
    this.importSites = importSites;
    this.convertProcess = convertProcess;
    this.fixesByProvider.put("Agassiz", this::fixAgassiz);
    this.fixesByProvider.put("Cranbrook", this::fixCranbrook);
    this.fixesByProvider.put("Chilliwack", this::fixChilliwack);
    this.fixesByProvider.put("CAPRD", this::fixCAPRD);
    this.fixesByProvider.put("CORD", this::fixCORD);
    this.fixesByProvider.put("CSHRD", this::fixCSHRD);
    this.fixesByProvider.put("FFGRD", this::fixFFGRD);
    this.fixesByProvider.put("North Vancouver City", this::fixNorthVancouverCity);
    this.fixesByProvider.put("Revelstoke", this::fixRevelstoke);
    this.fixesByProvider.put("Summerland", this::fixSummerland);
    this.inputFile = path;
    this.counts = convertProcess.counts;
    final String fileName = Paths.getBaseName(path);
    final String providerShortName = fileName.replace("_ADDRESS_BC", "");
    this.partnerOrganization = PartnerOrganizations
      .newPartnerOrganization(CaseConverter.toCapitalizedWords(providerShortName));

    this.allErrorLog = allErrorLog;
    this.allWarningLog = allWarningLog;
    this.ignoreNames.clear();
  }

  @Override
  public void addError(final Record record, final String message) {
    this.counts.addCount(this.partnerOrganization, BatchUpdateDialog.ERROR);
    this.importSites.addLabelCount(BatchUpdateDialog.ERROR, message, BatchUpdateDialog.ERROR);
    final Point point = record.getGeometry();

    this.allErrorLog.error(this.partnerOrganization.getPartnerOrganizationName(), message, record,
      point);
  }

  public void addWarning(final Record record, final String message) {
    this.counts.addCount(this.partnerOrganization, ProviderSitePointConverter.WARNING);
    this.importSites.addLabelCount(ProviderSitePointConverter.WARNING, message,
      ProviderSitePointConverter.WARNING);
    final Point point = record.getGeometry();
    this.allWarningLog.error(this.partnerOrganization.getPartnerOrganizationName(), message, record,
      point);
  }

  @Override
  public void addWarningCount(final String message) {
    addWarning(this.sourceRecord, message);
  }

  @Override
  public SitePointProviderRecord convert(final Record sourceRecord, final Point point) {
    this.sourceRecord = sourceRecord;
    // if (sourceRecord.equalValue(FULL_ADDRESS, "3042 XE PAY RD")) {
    // Debug.noOp();
    // }
    final AddressBcSite sourceSite = new AddressBcSite(this, sourceRecord, point);
    providerFix(sourceSite);
    if (sourceSite.fixSourceSite(this, this.localityName)) {
      return newSitePoint(sourceSite);
    } else {
      return null;
    }
  }

  private SitePointProviderRecord convertSite(final Record sourceRecord) {
    final Point sourcePoint = sourceRecord.getGeometry();
    if (Property.isEmpty(sourcePoint)) {
      addError(sourceRecord, "Record does not contain a point geometry");
    } else if (!sourcePoint.isValid()) {
      addError(sourceRecord, "Record does not contain a valid point geometry");
    } else {

      try {
        final Point point = Gba.GEOMETRY_FACTORY_2D_1M.point(sourcePoint);

        final Identifier localityId = LOCALITIES.getBoundaryId(point);
        setLocalityName(LOCALITIES.getValue(localityId));
        final SitePointProviderRecord sitePoint = convert(sourceRecord, point);
        if (sitePoint == null) {
          this.counts.addCount(this.partnerOrganization, ProviderSitePointConverter.IGNORED);
        } else {
          sitePoint.setValue(OPEN_DATA_IND, "N");
          sitePoint.setValue(LOCALITY_ID, localityId);

          sitePoint.updateFullAddress();
          final Identifier partnerOrgId = getPartnerOrganizationId();
          sitePoint.setValue(CREATE_PARTNER_ORG_ID, partnerOrgId);
          sitePoint.setValue(MODIFY_PARTNER_ORG_ID, partnerOrgId);
          sitePoint.setValue(CUSTODIAN_PARTNER_ORG_ID, partnerOrgId);
        }
        return sitePoint;
      } catch (final NullPointerException e) {
        Logs.error(ImportSites.class, "Null pointer", e);
        addError(sourceRecord, "Null Pointer");
        this.counts.addCount(this.partnerOrganization, ProviderSitePointConverter.IGNORED);
      } catch (final IgnoreSiteException e) {
        addError(sourceRecord, e.getMessage());
        this.counts.addCount(this.partnerOrganization, ProviderSitePointConverter.IGNORED);
      } catch (final Throwable e) {
        addError(sourceRecord, e.getMessage());
        this.counts.addCount(this.partnerOrganization, ProviderSitePointConverter.IGNORED);
      }
    }
    return null;
  }

  public boolean equalsShortName(final String shortName) {
    return this.partnerOrganization.equalsShortName(shortName);
  }

  private void fixAgassiz(final AddressBcSite site) {
    if (!site.isStructuredNameExists()) {
      if (site.equalValue(FULL_ADDRESS, "1 5736A KAMP RD")) {
        site.fullAddress = "1-5736A KAMP RD";
        site.unitDescriptor = RangeSet.newRangeSet(1);
        site.streetNumber = "5736";
        site.civicNumberSuffix = "A";
        site.nameBody = "KAMP";
        site.addWarning("[STREET_NUMBER] in [NAME_BODY]");
      } else if (Character.isDigit(site.nameBody.charAt(0))) {
        final int index = site.nameBody.indexOf(' ');
        if (index != -1) {
          final String number = site.nameBody.substring(0, index);
          site.nameBody = site.nameBody.substring(index + 1);
          site.unitDescriptor = RangeSet.newRangeSet(site.streetNumber);
          site.streetNumber = number;
          site.addWarning("[STREET_NUMBER] in [NAME_BODY]");
        }
      } else if (site.equalValue(FULL_ADDRESS, "5 A 7291 MORROW RD")) {
        site.fullAddress = "5A 7291 MORROW RD";
        site.nameBody = "MORROW";
        site.unitDescriptor = RangeSet.newRangeSet("5A");
        site.streetNumber = "7291";
        site.addWarning("[STREET_NUMBER] in [NAME_BODY]");
      }
    }
  }

  private void fixCAPRD(final AddressBcSite site) {
    if (site.fullAddress.startsWith("DOCK-")) {
      site.fullAddress = site.fullAddress.substring(5);
    }
  }

  private void fixChilliwack(final AddressBcSite site) {
    if (site.equalValue(FULL_ADDRESS, "1 MH-5121 EXTROM RD")) {
      site.fullAddress = "1-5121 EXTROM RD";
    }
  }

  private void fixCORD(final AddressBcSite site) {
    if (site.fullAddress.contains(" - 3994-3994")) {
      site.fullAddress = site.fullAddress.replace(" - 3994-3994", "-3994");
    }
  }

  private void fixCranbrook(final AddressBcSite site) {
    if (site.fullAddress.startsWith("BUILDING ")) {
      site.fullAddress = site.fullAddress.substring(9);
    }
  }

  private void fixCSHRD(final AddressBcSite site) {
    if (site.equalValue(FULL_ADDRESS, "41BH-4162 SQUILAX-ANGLEMONT RD")) {
      site.fullAddress = "41-4162 SQUILAX-ANGLEMONT RD";
    }
  }

  private void fixFFGRD(final AddressBcSite site) {
    if (site.fullAddress.equals("1B-5775 HAR-LEES PLACE RD")
      && site.unitDescriptor.toString().equals("1A,1B")) {
      site.fullAddress = site.fullAddress = "1A," + site.fullAddress;
    }
  }

  private void fixNorthVancouverCity(final AddressBcSite site) {
    if (site.equalValue(FULL_ADDRESS, "V 7 M 3P W 2ND ST")) {
      site.fullAddress = "7 W 2ND ST";
      site.unitDescriptor.clear();
      site.streetNumberPrefix = null;
      site.civicNumberSuffix = null;
    }
  }

  private void fixRevelstoke(final AddressBcSite site) {
    if (site.fullAddress.startsWith("A-") && site.fullAddress.endsWith("-1051 SANDSTONE RD")) {
      site.fullAddress = "A" + site.fullAddress.substring(2);
    }
  }

  private void fixSummerland(final AddressBcSite site) {
    if ("JUBILEE RD".equals(site.getStructuredName())) {
      String structuredName;
      if (Integer.parseInt(site.streetNumber) < 9500) {
        structuredName = "Jubilee Rd E";
      } else {
        structuredName = "Jubilee Rd W";
      }
      site.setStructuredNameAndParts(structuredName);
      site.fullAddress = site.fullAddress.replace("JUBILEE RD", structuredName);
      site.addressParts = site.fullAddress;
      site.addWarning("Mapped JUBILEE RD to " + structuredName);
    }
  }

  @Override
  protected StatisticsDialog getDialog() {
    return this.importSites;
  }

  @Override
  protected Identifier getPartnerOrganizationId() {
    return this.partnerOrganization.getPartnerOrganizationId();
  }

  @Override
  protected String getPartnerOrganizationShortName() {
    return this.partnerOrganization.getPartnerOrganizationShortName();
  }

  @Override
  public boolean isCancelled() {
    return this.convertProcess.isCancelled();
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
              this.counts.addCount(this.partnerOrganization, "Duplicate");
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
                } else if (range2.isEmpty()) {
                  Logs.error(this, "Not expecting range2 to be empty");
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
                this.counts.addCount(this.partnerOrganization, "Merged UD");
              }
            }
          }
        }
      }
    }
  }

  public SitePointProviderRecord newSitePoint(final AddressBcSite sourceSite) {
    final Point point = sourceSite.getPoint();
    final SitePointProviderRecord sitePoint = newSitePoint(point);
    sitePoint.setValue(SITE_LOCATION_CODE, sourceSite.siteLocationCode);

    final String partnerOrganizationShortName = getPartnerOrganizationShortName();
    final String structuredName = sourceSite.getStructuredName();
    final FeatureStatus featureStatusCode = Maps.getMap(featureStatusByLocalityAndStreetName,
      partnerOrganizationShortName.toUpperCase(), structuredName, FeatureStatus.ACTIVE);
    if (featureStatusCode.isIgnored()) {
      final String message = "Ignored STREET_NAME in STRUCTURED_NAME_ALIAS.xlsx";
      addStructuredNameError(partnerOrganizationShortName, message, structuredName, null, null);
      sourceSite.addWarning(message);
      return null;
    } else if (!setStructuredName(sourceSite, sitePoint, 0, structuredName, structuredName)) {
      return null;
    }
    final String updatedStructuredName = sitePoint.getStructuredName();
    if (updatedStructuredName != null) {
      sourceSite.setStructuredName(updatedStructuredName);
    }
    if (Property.hasValue(sourceSite.getAliasName())) {
      if (!setStructuredName(sourceSite, sitePoint, 1, sourceSite.getAliasName(), structuredName)) {
        return null;
      }
    }
    if (Property.hasValue(sourceSite.civicNumberSuffix)) {
      if (sourceSite.civicNumberSuffix.matches("([A-Z]|1/2)")) {
        // Debug.noOp();
      } else if (Integers.isInteger(sourceSite.civicNumberSuffix)) {
        sourceSite.unitDescriptor = RangeSet.newRangeSet(sourceSite.civicNumberSuffix);
        sourceSite.civicNumberSuffix = null;
        addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR numeric");
      } else if (sourceSite.civicNumberSuffix.matches("[A-Z]\\d+")
        || sourceSite.civicNumberSuffix.matches("\\d+[A-Z]")) {
        sourceSite.unitDescriptor = RangeSet.newRangeSet(
          Strings.toString(",", sourceSite.unitDescriptor, sourceSite.civicNumberSuffix));
        sourceSite.civicNumberSuffix = "";
        addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
      } else if (sourceSite.civicNumberSuffix.matches("[A-Z0-9]-[A-Z0-9]")) {
        sourceSite.unitDescriptor = RangeSet.newRangeSet(
          Strings.toString(",", sourceSite.unitDescriptor, sourceSite.civicNumberSuffix));
        sourceSite.civicNumberSuffix = "";
        addWarningCount("CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR range");
      } else if (sourceSite.civicNumberSuffix.matches("-[A-Z]")) {
        sourceSite.civicNumberSuffix = sourceSite.civicNumberSuffix.substring(1);
        addError(sourceSite, "CIVIC_NUMBER_SUFFIX includes - prefix");
      } else if (sourceSite.civicNumberSuffix.equals(".5")) {
        sourceSite.civicNumberSuffix = "1/2";
        addWarning(sourceSite, "CIVIC_NUMBER_SUFFIX=0.5 should be 1/2");
      } else if (sourceSite.civicNumberSuffix.startsWith("/")
        || sourceSite.civicNumberSuffix.startsWith("&")) {
        sourceSite.civicNumberRange = sourceSite.streetNumber + ","
          + sourceSite.civicNumberSuffix.substring(1);
        sourceSite.streetNumber = null;
        sourceSite.civicNumberSuffix = null;
        addError(sourceSite, "CIVIC_NUMBER_SUFFIX is a range starting with / or &");
      } else {
        Debug.noOp();
      }
    }
    sitePoint.setValue(UNIT_DESCRIPTOR, sourceSite.unitDescriptor);
    sitePoint.setValue(CIVIC_NUMBER, sourceSite.getCivicNumber());
    sitePoint.setValue(CIVIC_NUMBER_SUFFIX, sourceSite.civicNumberSuffix);
    sitePoint.setValue(POSTAL_CODE, sourceSite.getString(POSTAL_CODE));
    sitePoint.setValue(SITE_TYPE_CODE, sourceSite.getString(AddressBc.BUILDING_TYPE));
    sitePoint.setValue(SITE_NAME_1, sourceSite.getString(AddressBc.BUILDING_NAME));

    if (Property.hasValue(sourceSite.civicNumberRange)) {
      sitePoint.setValue(CIVIC_NUMBER_RANGE, sourceSite.civicNumberRange.replace('-', '~'));
    }

    final String finalFullAddress = sourceSite.getNewFullAddress();
    sitePoint.setValue(SitePoint.FULL_ADDRESS, finalFullAddress);

    final String custodianFullAddress = sourceSite.getString(FULL_ADDRESS);
    sitePoint.setValue(CUSTODIAN_FULL_ADDRESS, custodianFullAddress);
    if (!sourceSite.extendedData.isEmpty()) {
      sitePoint.setValue(EXTENDED_DATA, Json.toString(sourceSite.extendedData));
    }
    // SitePoint.updateCustodianSiteId("addressBc", sitePoint);
    return sitePoint;
  }

  public void providerFix(final AddressBcSite sourceSite) {
    final Consumer<AddressBcSite> providerFixes = this.fixesByProvider
      .get(getPartnerOrganizationShortName());
    if (providerFixes != null) {
      providerFixes.accept(sourceSite);
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
    final RecordDefinitionImpl siteRecordDefinition = ProviderSitePointConverter
      .getSitePointTsvRecordDefinition();
    final String shortName = getPartnerOrganizationShortName();
    final String baseName = BatchUpdateDialog.toFileName(shortName);

    final Path file = this.importSites.getSitePointByProviderDirectory() //
      .resolve(baseName + "_SITE_POINT_ADDRESS_BC.tsv");
    try (
      RecordReader reader = RecordReader.newRecordReader(this.inputFile);
      RecordWriter sitePointWriter = this.sitePointWriter = RecordWriter
        .newRecordWriter(siteRecordDefinition, file);) {
      this.sitePointWriter.setProperty("useQuotes", false);

      final Map<String, Map<Integer, List<SitePointProviderRecord>>> sitesByStreetAddress = new TreeMap<>();

      for (final Record sourceRecord : cancellable(reader)) {
        this.counts.addCount(this.partnerOrganization, "Read");
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
    } catch (final Throwable e) {
      Logs.error(this, e);
    }
  }

  public void setLocalityName(final String localityName) {
    this.localityName = localityName;
  }

  public void writeSitePoint(final Record record) {
    final Record writeRecord = this.sitePointWriter.newRecord(record);
    writeRecord.setGeometryValue(record);
    this.sitePointWriter.write(writeRecord);
    this.counts.addCount(this.partnerOrganization, "Write");
  }
}
