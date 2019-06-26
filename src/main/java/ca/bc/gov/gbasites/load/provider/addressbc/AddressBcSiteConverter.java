package ca.bc.gov.gbasites.load.provider.addressbc;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.jeometry.common.number.Integers;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.PartnerOrganizationFiles;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.geometry.model.Point;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class AddressBcSiteConverter extends AbstractSiteConverter {

  public static void convertAll(final StatisticsDialog dialog, final boolean convert) {

    final List<PartnerOrganization> partnerOrganizations = ImportSites.SOURCE_BY_PROVIDER
      .listPartnerOrganizations(AddressBc.ADDRESS_BC_DIRECTORY, AddressBc.FILE_SUFFIX);

    if (!partnerOrganizations.isEmpty()) {

      final RecordDefinition recordDefinition;
      {
        final PartnerOrganization partnerOrganization = partnerOrganizations.get(0);
        final Path firstFile = ImportSites.SOURCE_BY_PROVIDER
          .getFilePath(AddressBc.ADDRESS_BC_DIRECTORY, partnerOrganization, AddressBc.FILE_SUFFIX);
        try (
          RecordReader reader = RecordReader.newRecordReader(firstFile)) {
          recordDefinition = reader.getRecordDefinition();
        }

      }
      try (
        RecordLog allErrorLog = newAllRecordLog(AddressBc.ADDRESS_BC_DIRECTORY, recordDefinition,
          "ERROR");
        RecordLog allWarningLog = newAllRecordLog(AddressBc.ADDRESS_BC_DIRECTORY, recordDefinition,
          "WARNING");) {
        final StructuredNames structuredNames = GbaController.getStructuredNames();
        structuredNames.setLoadAll(true);
        structuredNames.setLoadMissingCodes(false);
        structuredNames.refresh();

        AbstractSiteConverter.init();

        final ProcessNetwork processNetwork = new ProcessNetwork();
        for (int i = 0; i < 8; i++) {
          processNetwork.addProcess(() -> {
            while (!dialog.isCancelled()) {
              PartnerOrganization partnerOrganization;
              synchronized (partnerOrganizations) {
                if (partnerOrganizations.isEmpty()) {
                  return;
                }
                partnerOrganization = partnerOrganizations.remove(0);
              }
              final AddressBcSiteConverter converter = new AddressBcSiteConverter(dialog,
                partnerOrganization, allErrorLog, allWarningLog);
              converter.convertSourceRecords(convert);
            }
          });
        }
        processNetwork.startAndWait();
      }
    }
  }

  public static String getCleanStringIntern(final Record record, final String fieldName) {
    String value = record.getString(fieldName);
    value = Strings.cleanWhitespace(value);
    if (value != null) {
      value = value.toUpperCase();
      value = value.intern();
    }
    return value;
  }

  public static RecordLog newAllRecordLog(final Path directory,
    final RecordDefinition recordDefinition, final String suffix) {
    final Path allErrorFile = directory.resolve("ADDRESS_BC_CONVERT_" + suffix + ".tsv");
    return new RecordLog(allErrorFile, recordDefinition, true);
  }

  private final Map<String, Consumer<AddressBcSite>> fixesByProvider = new HashMap<>();

  private RecordWriter sitePointWriter;

  private final RecordLog allErrorLog;

  private final RecordLog allWarningLog;

  public AddressBcSiteConverter(final StatisticsDialog dialog,
    final PartnerOrganization partnerOrganization, final RecordLog allErrorLog,
    final RecordLog allWarningLog) {
    setCountPrefix("ABC ");
    setFileSuffix(AddressBc.FILE_SUFFIX);
    setBaseDirectory(AddressBc.ADDRESS_BC_DIRECTORY);

    this.createModifyPartnerOrganization = AddressBc.getAbcPartnerOrganization();

    final PartnerOrganizationFiles partnerOrganizationFiles = new PartnerOrganizationFiles(dialog,
      partnerOrganization, AddressBc.ADDRESS_BC_DIRECTORY, AddressBc.FILE_SUFFIX);
    setPartnerOrganizationFiles(partnerOrganizationFiles);

    setDialog(dialog);

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

    this.allErrorLog = allErrorLog;
    this.allWarningLog = allWarningLog;
    this.ignoreNames.clear();
  }

  @Override
  public void addError(final Record record, final String message) {
    super.addError(record, message);

    final Point point = record.getGeometry();
    this.allErrorLog.error(getPartnerOrganizationName(), message, record, point);
  }

  @Override
  public void addWarning(final Record record, final String message) {
    super.addWarning(record, message);
    final Point point = record.getGeometry();
    this.allWarningLog.error(getPartnerOrganizationName(), message, record, point);
  }

  @Override
  public SitePointProviderRecord convertRecordSite(final Record sourceRecord, final Point point) {
    // if (sourceRecord.equalValue(FULL_ADDRESS, "3042 XE PAY RD")) {
    // Debug.noOp();
    // }
    final AddressBcSite sourceSite = new AddressBcSite(this, sourceRecord, point);
    providerFix(sourceSite);
    if (sourceSite.fixSourceSite(this, this.localityName)) {
      return newSitePoint(sourceRecord, sourceSite);
    } else {
      return null;
    }
  }

  public boolean equalsShortName(final String shortName) {
    return getPartnerOrganization().equalsShortName(shortName);
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

  private SitePointProviderRecord newSitePoint(final Record sourceRecord,
    final AddressBcSite sourceSite) {
    final Point point = sourceSite.getPoint();
    final SitePointProviderRecord sitePoint = newSitePoint(this, point);
    sitePoint.setValue(SITE_LOCATION_CODE, sourceSite.siteLocationCode);

    final String partnerOrganizationShortName = getPartnerOrganizationShortName();
    final String structuredName = sourceSite.getStructuredName();
    final FeatureStatus featureStatusCode = Maps.getMap(featureStatusByLocalityAndStreetName,
      partnerOrganizationShortName.toUpperCase(), structuredName, FeatureStatus.ACTIVE);
    if (featureStatusCode.isIgnored()) {
      final String message = "Ignored STREET_NAME in STRUCTURED_NAME_ALIAS.xlsx";
      addStructuredNameError(message, structuredName, null, null);
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
        addWarning(sourceRecord, "CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR numeric");
      } else if (sourceSite.civicNumberSuffix.matches("[A-Z]\\d+")
        || sourceSite.civicNumberSuffix.matches("\\d+[A-Z]")) {
        sourceSite.unitDescriptor = RangeSet.newRangeSet(
          Strings.toString(",", sourceSite.unitDescriptor, sourceSite.civicNumberSuffix));
        sourceSite.civicNumberSuffix = "";
        addWarning(sourceRecord, "CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR");
      } else if (sourceSite.civicNumberSuffix.matches("[A-Z0-9]-[A-Z0-9]")) {
        sourceSite.unitDescriptor = RangeSet.newRangeSet(
          Strings.toString(",", sourceSite.unitDescriptor, sourceSite.civicNumberSuffix));
        sourceSite.civicNumberSuffix = "";
        addWarning(sourceRecord, "CIVIC_NUMBER_SUFFIX is UNIT_DESCRIPTOR range");
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

  public void setLocalityName(final String localityName) {
    this.localityName = localityName;
  }

  public void writeSitePoint(final Record record) {
    final Record writeRecord = this.sitePointWriter.newRecord(record);
    writeRecord.setGeometryValue(record);
    this.sitePointWriter.write(writeRecord);
  }
}