package ca.bc.gov.gbasites.load.provider.geobc;

import java.nio.file.Path;
import java.util.List;

import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.convert.ConvertAllRecordLog;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBC;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSite;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSiteConverter;

import com.revolsys.collection.range.RangeSet;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Debug;
import com.revolsys.util.Strings;

public class GeoBcSiteConverter extends AddressBcSiteConverter {

  public static void convertAll(final StatisticsDialog dialog) {

    final List<PartnerOrganization> partnerOrganizations = ImportSites.SOURCE_BY_PROVIDER
      .listPartnerOrganizations(GeoBC.DIRECTORY, GeoBC.FILE_SUFFIX);

    if (!partnerOrganizations.isEmpty()) {

      try (
        ConvertAllRecordLog allLog = new ConvertAllRecordLog(GeoBC.DIRECTORY, GeoBC.FILE_SUFFIX)) {

        final ProcessNetwork processNetwork = new ProcessNetwork();
        for (int i = 0; i < 8; i++) {
          processNetwork.addProcess(GeoBC.NAME + " Convert " + (i + 1), () -> {
            while (!dialog.isCancelled()) {
              PartnerOrganization partnerOrganization;
              synchronized (partnerOrganizations) {
                if (partnerOrganizations.isEmpty()) {
                  return;
                }
                partnerOrganization = partnerOrganizations.remove(0);
              }
              try {
                final GeoBcSiteConverter converter = new GeoBcSiteConverter(dialog,
                  partnerOrganization, allLog);
                converter.convertSourceRecords();
              } catch (final Exception e) {
                Logs.error(GeoBcSiteConverter.class,
                  GeoBC.NAME + ": Error converting for: " + partnerOrganization, e);
              }
            }
          });
          ;
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

  public GeoBcSiteConverter(final StatisticsDialog dialog,
    final PartnerOrganization partnerOrganization, final ConvertAllRecordLog allLog) {
    super(dialog, partnerOrganization, allLog, GeoBC.DIRECTORY, GeoBC.FILE_SUFFIX,
      GeoBC.COUNT_PREFIX);
    this.createModifyPartnerOrganization = GeoBC.PARTNER_ORGANIZATION;
  }

  @Override
  protected SitePointProviderRecord convertRecordDo(final Record sourceRecord) {
    final SitePointProviderRecord convertedRecord = super.convertRecordDo(sourceRecord);
    if (convertedRecord == null) {
      super.convertRecordDo(sourceRecord);
    }
    return convertedRecord;
  }

  protected void fixStreetNumber(final AddressBcSite sourceSite, final String streetNumberPart) {
    try {
      final int civicNumber = Integer.parseInt(streetNumberPart);
      sourceSite.streetNumber = Integer.toString(civicNumber);
    } catch (final Exception e) {
      Debug.noOp();
      // Ignore
    }
  }

  private void fixStreetNumberPart(final AddressBcSite sourceSite, String firstPart) {
    if (sourceSite.civicNumberSuffix == null) {
      final int endIndex = firstPart.length() - 1;
      if (!Character.isDigit(firstPart.charAt(endIndex))) {
        final String civicNumberSuffix = firstPart.substring(endIndex);
        sourceSite.civicNumberSuffix = civicNumberSuffix;
        fixStreetNumber(sourceSite, firstPart.substring(0, endIndex));
      } else {
        fixStreetNumber(sourceSite, firstPart);
      }
    } else if (firstPart.endsWith(sourceSite.civicNumberSuffix)) {
      firstPart = firstPart.substring(0,
        firstPart.length() - sourceSite.civicNumberSuffix.length());
      fixStreetNumber(sourceSite, firstPart);
    }
  }

  private void fixStreetNumberUnit(final AddressBcSite sourceSite, final String unit,
    final String number) {
    if (sourceSite.unitDescriptor.isEmpty()) {
      sourceSite.unitNumber = unit;
      sourceSite.unitDescriptor = RangeSet.newRangeSet(unit);
    } else if (!unit.equals(sourceSite.unitNumber)) {
      Debug.noOp();
    }
    fixStreetNumberPart(sourceSite, number);
  }

  @Override
  public void providerFix(final AddressBcSite sourceSite) {
    String fullAddress = sourceSite.getFullAddress();
    final String streetType = sourceSite.nameSuffixCode;
    {
      String nameBody = sourceSite.nameBody;
      if (nameBody != null) {
        if (nameBody.indexOf(", ") != -1) {
          nameBody = nameBody.replace(", ", " ");
          sourceSite.nameBody = nameBody;
        }
        if (nameBody.endsWith(",")) {
          nameBody = nameBody.substring(0, nameBody.length() - 1);
        }
        if (streetType != null) {
          final int spaceIndex = nameBody.length() - streetType.length() - 1;
          if (nameBody.endsWith(streetType) && nameBody.charAt(spaceIndex) == ' '
            && spaceIndex > 0) {
            nameBody = nameBody.substring(0, spaceIndex);
            if (fullAddress != null) {
              if (fullAddress.endsWith(" " + streetType + " " + streetType)) {
                fullAddress = fullAddress.substring(0,
                  fullAddress.length() - (streetType.length() + 1));
              }
            }
          }
        }
      }
    }

    if (fullAddress != null) {
      final int lastCommaSpace = fullAddress.lastIndexOf(", ");
      if (lastCommaSpace != -1) {
        final String geographicDomain = sourceSite.getString(AddressBC.GEOGRAPHIC_DOMAIN);
        final String lastPart = fullAddress.substring(lastCommaSpace + 2);
        if (lastPart.startsWith("PORT ALBERNI") || lastPart.equalsIgnoreCase("UCLUELET")
          || lastPart.equalsIgnoreCase("TOFINO") || lastPart.equalsIgnoreCase("SICAMOUS")
          || lastPart.equalsIgnoreCase(geographicDomain)) {
          fullAddress = fullAddress.substring(0, lastCommaSpace);
        }
      }

      if (fullAddress.indexOf(", ") != -1) {
        fullAddress = fullAddress.replace(", ", " ");
      }
      if (fullAddress.endsWith(",")) {
        fullAddress = fullAddress.substring(0, fullAddress.length() - 1);
      }

      if ("BLVD".equalsIgnoreCase(streetType) && fullAddress.endsWith("BL")) {
        fullAddress += "VD";
      }
      if ("DRIVE".equalsIgnoreCase(streetType) && fullAddress.endsWith("DR")) {
        sourceSite.nameSuffixCode = "DR";
      }
      if ("PLACE".equalsIgnoreCase(streetType) && fullAddress.endsWith("PL")) {
        sourceSite.nameSuffixCode = "PL";
      }
      if ("COURT".equalsIgnoreCase(streetType) && fullAddress.endsWith("CRT")) {
        sourceSite.nameSuffixCode = "CRT";
      }
      if ("ROAD".equalsIgnoreCase(streetType) && fullAddress.endsWith("RD")) {
        sourceSite.nameSuffixCode = "RD";
      }
      if (sourceSite.unitNumber != null
        && fullAddress.startsWith(sourceSite.unitNumber + '-' + sourceSite.unitNumber + '-')) {
        fullAddress = fullAddress.substring(fullAddress.indexOf('-') + 1);
      }

      if (sourceSite.streetNumber == null && !fullAddress.equals(sourceSite.getStructuredName())) {
        final int spaceIndex = fullAddress.indexOf(' ');
        if (spaceIndex != -1) {
          final String firstPart = fullAddress.substring(0, spaceIndex);
          if (firstPart.equals(sourceSite.civicNumberSuffix)) {
            sourceSite.streetNumber = sourceSite.civicNumberSuffix;
            sourceSite.civicNumberSuffix = null;
          } else {
            final String[] parts = firstPart.split("-");
            final String part1 = parts[0];
            if (parts.length == 1) {
              fixStreetNumberPart(sourceSite, part1);
            } else if (parts.length == 2) {
              fixStreetNumberUnit(sourceSite, part1, parts[1]);
            } else if (parts.length == 3) {
              final String part2 = parts[1];
              final String part3 = parts[2];
              if (part1.equals(part2)) {
                fixStreetNumberUnit(sourceSite, part1, part3);
                fullAddress = fullAddress.substring(part1.length() + 1);
              } else if (sourceSite.unitNumber == null || part1.equals(sourceSite.unitNumber)) {
                final String unit = part1 + "-" + part2;
                sourceSite.unitDescriptor.clear();
                fixStreetNumberUnit(sourceSite, unit, part3);
              } else {
                Debug.noOp();
              }
            } else {
              Debug.noOp();
            }
          }
        }
      }
      sourceSite.setFullAddress(fullAddress);
    }
  }
}
