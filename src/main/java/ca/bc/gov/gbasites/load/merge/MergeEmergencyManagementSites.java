package ca.bc.gov.gbasites.load.merge;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.LoadProviderSitesIntoGba;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.model.type.code.CommunityPoly;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;
import ca.bc.gov.gbasites.model.type.code.SiteLocationCode;

import com.revolsys.geometry.model.Point;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.io.Reader;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.count.LabelCountMap;
import com.revolsys.util.count.LabelCounters;

public class MergeEmergencyManagementSites implements SitePoint {

  public static final String IGNORE_XCOVER = "Ignore XCOVER";

  public static final String EM_SITES = "Em Sites";

  private final BoundaryCache communityCache = CommunityPoly.getCommunities();

  private final BoundaryCache localityCache = GbaController.getLocalities();

  private final BoundaryCache regionalDistrictCache = GbaController.getRegionalDistricts();

  private final Map<Identifier, String> emMissingNameBySiteId = Identifier.newTreeMap();

  private final List<Object[]> emSiteDifferentCityFromLocalities = new ArrayList<>();

  private final List<Object[]> emWrongSiteType = new ArrayList<>();

  private final ImportSites dialog;

  private final LabelCounters typeCounts = new LabelCountMap();

  public MergeEmergencyManagementSites(final ImportSites dialog) {
    this.dialog = dialog;
  }

  private Identifier getNameId(final Identifier localityId, final Identifier siteId, String name) {
    Identifier structuredNameId = null;
    name = name.trim().replaceAll("\\s+", " ");
    if (Property.hasValue(name)) {
      structuredNameId = GbaController.structuredNames.getIdentifier(name);
      if (structuredNameId == null) {
        final List<Identifier> nameIds = GbaController.structuredNames.getMatchingNameIds(name);
        if (nameIds.size() == 1) {
          structuredNameId = nameIds.get(0);
        } else if (nameIds.isEmpty()) {
          this.emMissingNameBySiteId.put(siteId, name);
        } else {
          for (final Iterator<Identifier> nameIdIter = nameIds.iterator(); nameIdIter.hasNext();) {
            final Identifier nameId = nameIdIter.next();
            final String matchName = GbaController.structuredNames.getValue(nameId);
            if (matchName.equals(name)) {
              return nameId;
            } else if (!name.replaceAll("[^A-Za-z0-9 ]", "")
              .equalsIgnoreCase(matchName.replaceAll("[^A-Za-z0-9 ]", ""))) {
              nameIdIter.remove();
            }
          }
          if (nameIds.size() == 1) {
            structuredNameId = nameIds.get(0);
          } else {
            this.emMissingNameBySiteId.put(siteId, name);
          }
        }
      }
    }
    return structuredNameId;
  }

  private Record loadEmergencyManagementSite(final RecordStore gbaRecordStore,
    final Record draSite) {
    final Identifier id = draSite.getIdentifier("ID");
    final String plType = Strings.trim(draSite.getString("PL_TYPE"));
    if ("XCOVER".equals(plType)) {
      this.dialog.addLabelCount(EM_SITES, SiteTables.SITE_POINT, IGNORE_XCOVER);
      return null;
    } else {
      final String plGroup = Strings.trim(draSite.getString("PL_GROUP"));
      this.typeCounts.addCount(plGroup + " " + plType);
      String siteName1 = Strings.trim(draSite.getString("PL_NAME"));
      String civicNumber = Strings.trim(draSite.getString("ADDR_NUM"));
      String name = Strings.trim(draSite.getString("ADDR_ROAD"));
      final String addrOther = Strings.trim(draSite.getString("ADDR_OTHER"));
      final String city = Strings.trim(draSite.getString("CITY"));
      @SuppressWarnings("unused")
      final String MAP_CHAR = Strings.trim(draSite.getString("MAP_CHAR"));
      final String siteName2 = Strings.trim(draSite.getString("PL_ALIAS"));
      final String siteName3 = Strings.trim(draSite.getString("PL_ALIAS2"));
      Point point = draSite.getGeometry();
      final double x = point.getX();
      final double y = point.getY();
      point = Gba.GEOMETRY_FACTORY_2D_1M.point(x, y);

      final Record site = gbaRecordStore.newRecord(SiteTables.SITE_POINT);

      final Map<String, Object> extendedData = new LinkedHashMap<>();

      String unitDescriptor = null;
      String civicNumberSuffix = null;
      String addressComment = null;
      if (Property.hasValue(civicNumber)) {
        civicNumber = civicNumber.trim();
        if (civicNumber.matches("\\d+")) {
        } else if (civicNumber.matches("\\d+ ?[A-Za-z]")) {
          civicNumberSuffix = civicNumber.substring(civicNumber.length() - 1);
          civicNumber = civicNumber.substring(0, civicNumber.length() - 1).trim();
        } else if (civicNumber.matches("[A-Z]?\\d+[-| ]+\\d+")) {
          final String[] parts = civicNumber.split("[-| ]+");
          unitDescriptor = parts[0];
          civicNumber = parts[1];
        } else if (civicNumber.matches("\\d+\\s+-\\s+\\d+")) {
          final String[] parts = civicNumber.split("\\s+-\\s+");
          unitDescriptor = parts[0];
          civicNumber = parts[1];
        } else if (civicNumber.matches("\\d+/\\d+")) {
          final String[] parts = civicNumber.split("/");
          civicNumber = parts[0];
          unitDescriptor = parts[1];
        } else if (civicNumber.matches("(Mile|MI) .+")) {
          addressComment = civicNumber;
          civicNumber = null;
        } else {
          unitDescriptor = civicNumber;
          civicNumber = null;
        }
      }
      if (Property.hasValue(addrOther)) {
        String unit = null;
        if (addrOther.matches("Unit \\d+")) {
          unit = addrOther.substring(5);
        } else if (addrOther.matches("#\\d+")) {
          unit = addrOther.substring(1);
        } else if (addrOther.matches("\\d+")) {
          unit = addrOther;
        } else if (addrOther.matches("Box \\d+")) {
          addressComment = "PO " + addrOther;
        } else if (addrOther.matches("Bag \\d+")) {
          addressComment = addrOther;
        } else if (addrOther.matches("PO Box \\d+")) {
          addressComment = addrOther;
        } else if (addrOther.toUpperCase().matches("[A-D]")) {
          civicNumberSuffix = addrOther.toUpperCase();
        } else if (addrOther.matches("\\d+-" + civicNumber + " " + name)) {
          unit = addrOther.substring(0, addrOther.indexOf('-'));
        } else {
          addressComment = addrOther;
        }
        unitDescriptor = Strings.toString(",", unitDescriptor, unit);
      }

      final String siteTypeCode = ImportSites.siteTypeByBuildingType.get(plGroup + "-" + plType);
      if (siteTypeCode == null) {
        this.emWrongSiteType.add(new Object[] {
          id, plGroup, plType
        });
      }

      final Identifier localityId = this.localityCache.setBoundaryId(site, point);

      this.communityCache.setBoundaryId(site, point);
      this.regionalDistrictCache.setBoundaryId(site, point);

      Identifier structuredNameId = null;
      Identifier aliasStructuredNameId = null;
      if (Property.hasValue(name)) {
        final int slashIndex = name.indexOf('/');
        if (slashIndex == -1) {
          structuredNameId = getNameId(localityId, id, name);
          if (structuredNameId == null) {
            extendedData.put("NAME", name);
          } else {
            name = GbaController.structuredNames.getValue(structuredNameId);
          }
        } else {
          final String name1 = name.substring(0, slashIndex).trim();
          final String name2 = name.substring(slashIndex + 1).trim();

          structuredNameId = getNameId(localityId, id, name1);
          if (structuredNameId == null) {
            name = name1;
            extendedData.put("NAME", name1);
          } else {
            name = GbaController.structuredNames.getValue(structuredNameId);
          }
          aliasStructuredNameId = getNameId(localityId, id, name2);
          if (aliasStructuredNameId == null) {
            extendedData.put("ALIAS_NAME", name2);
          }
        }
      }

      site.setIdentifier(id);
      site.setValue(CUSTODIAN_SITE_ID, id);
      site.setValue(SITE_LOCATION_CODE, SiteLocationCode.PARCEL);
      site.setValue(SITE_TYPE_CODE, siteTypeCode);
      if (Property.hasValue(unitDescriptor)) {
        unitDescriptor = Strings.upperCase(unitDescriptor).replace('-', '~');
        site.setValue(UNIT_DESCRIPTOR, unitDescriptor);
      }
      if (Property.hasValue(civicNumber)) {
        site.setValue(CIVIC_NUMBER, civicNumber);
      }
      if (Property.hasValue(civicNumberSuffix)) {
        site.setValue(CIVIC_NUMBER_SUFFIX, Strings.upperCase(civicNumberSuffix));
      }
      site.setValue(STREET_NAME_ID, structuredNameId);
      site.setValue(STREET_NAME_ALIAS_1_ID, aliasStructuredNameId);
      SitePoint.updateFullAddress(site);
      String fullAddress = site.getString(FULL_ADDRESS);

      String useSiteNameInAddress = "N";
      boolean useInAddressRange = true;

      if (!Property.hasValue(fullAddress)) {
        useInAddressRange = false;
        if (Property.hasValue(siteName1)) {
          fullAddress = siteName1;
          useSiteNameInAddress = "Y";
        } else {
          fullAddress = this.localityCache.getValue(localityId) + " "
            + gbaRecordStore.getCodeTable(SiteTables.SITE_TYPE_CODE).getValue(siteTypeCode);
          siteName1 = fullAddress;
          useSiteNameInAddress = "Y";
        }
        site.setValue(FULL_ADDRESS, fullAddress);
      }
      if (Property.hasValue(siteName1)) {
        site.setValue(SITE_NAME_1, siteName1);
      }
      if (Property.hasValue(siteName2)) {
        site.setValue(SITE_NAME_2, siteName2);
      }
      if (Property.hasValue(siteName3)) {
        site.setValue(SITE_NAME_3, siteName3);
      }
      site.setValue(EMERGENCY_MANAGEMENT_SITE_IND, "Y");
      SitePoint.setUseInAddressRange(site, useInAddressRange);
      site.setValue(USE_SITE_NAME_IN_ADDRESS_IND, useSiteNameInAddress);
      site.setValue(FEATURE_STATUS_CODE, FeatureStatus.ACTIVE);
      site.setValue(ADDRESS_COMMENT, addressComment);
      site.setValue(CREATE_PARTNER_ORG_ID, 3);
      site.setValue(MODIFY_PARTNER_ORG_ID, 3);
      site.setValue(CUSTODIAN_PARTNER_ORG_ID, 3);
      site.setValue(OPEN_DATA_IND, "N");

      if (localityId == null) {
        extendedData.put("CITY", city);
        this.emSiteDifferentCityFromLocalities.add(new Object[] {
          id.toString(), fullAddress, city, null
        });
      } else {
        final String localityName = this.localityCache.getValue(localityId);
        if (!localityName.equalsIgnoreCase(city)) {
          extendedData.put("CITY", city);
          this.emSiteDifferentCityFromLocalities.add(new Object[] {
            id.toString(), fullAddress, city, localityName
          });
        }
      }

      if (!extendedData.isEmpty()) {
        final String data = Json.toString(extendedData);
        site.setValue(EXTENDED_DATA, data);
      }
      site.setGeometryValue(point);

      return site;
    }
  }

  private void loadEmergencyManagementSites() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    final Query loadedQuery = new Query(SiteTables.SITE_POINT, Q.lessThan(SITE_ID, 100000));
    loadedQuery.setStatistics(this.dialog.getLabelCountMap(EM_SITES, BatchUpdateDialog.GBA_READ));
    loadedQuery.setCancellable(this.dialog);
    final RecordReader recordReader = gbaRecordStore.getRecords(loadedQuery);
    final Map<Identifier, Record> recordsById = recordReader.readRecordsById();

    final List<Record> duplicateIdentifierRecords = new ArrayList<>();
    final Map<Identifier, Record> readEmSitesById = new HashMap<>();
    try (
      RecordStore emRecordStore = FileGdbRecordStoreFactory
        .newRecordStore(GbaController.getAncillaryDirectory("sites.gdb"))) {
      emRecordStore.initialize();
      final Query query = new Query("/sites");
      query.setCancellable(this.dialog);
      query.addOrderBy("ID", true);
      try (
        Reader<Record> emReader = emRecordStore.getRecords(query)) {
        for (final Record emSite : this.dialog.cancellable(emReader)) {
          this.dialog.addLabelCount(EM_SITES, SiteTables.SITE_POINT,
            LoadProviderSitesIntoGba.PROVIDER_READ);
          final Record site = loadEmergencyManagementSite(gbaRecordStore, emSite);
          if (site != null) {
            final Identifier identifier = site.getIdentifier();
            final Record readEmSite = readEmSitesById.get(identifier);
            if (readEmSite == null) {
              readEmSitesById.put(identifier, site);
              final Record gbaSite = recordsById.get(identifier);
              try (
                Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW)) {
                if (gbaSite == null) {
                  this.dialog.insertRecord(EM_SITES, site);
                } else {
                  this.dialog.updateRecord(EM_SITES, gbaSite, site);
                }
              } catch (final Throwable e) {
                Logs.error("Error processing site: " + site, e);
              }
            } else {
              duplicateIdentifierRecords.add(readEmSite);
              duplicateIdentifierRecords.add(site);
            }
          }
        }
      }
    }
    logEmergencyManagementSiteDuplicateId(gbaRecordStore, duplicateIdentifierRecords);
    logEmergencyManagementSiteMissingNames();
    logEmergencyManagementSiteDifferentLocalities();
    logEmergencyManagementSiteWrongSiteType();
  }

  private void logEmergencyManagementSiteDifferentLocalities() {
    final String fileName = "em_site_city_locality_different.xlsx";
    final Path file = ImportSites.SITES_DIRECTORY.resolve(fileName);
    if (this.emSiteDifferentCityFromLocalities.isEmpty()) {
      Paths.deleteDirectories(file);
    } else {
      final RecordDefinition recordDefinition = new RecordDefinitionBuilder("ERROR")//
        .addField("SITE_ID", DataTypes.INT) //
        .addField(FULL_ADDRESS, DataTypes.STRING, 20) //
        .addField("CITY", DataTypes.STRING, 28) //
        .addField("LOCALITY_NAME", DataTypes.STRING, 28) //
        .getRecordDefinition();
      try (
        final RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file)) {
        for (final Object[] record : this.emSiteDifferentCityFromLocalities) {
          writer.write(record);
        }
        Logs.warn(this, "Check log " + fileName);
      }
    }
  }

  private void logEmergencyManagementSiteDuplicateId(final RecordStore gbaRecordStore,
    final List<Record> duplicateIdentifierRecords) {
    final String fileName = "em_site_duplicate_id.tsv";
    final Path file = ImportSites.SITES_DIRECTORY.resolve(fileName);
    if (duplicateIdentifierRecords.isEmpty()) {
      Paths.deleteDirectories(file);
    } else {
      final RecordDefinition recordDefinition = gbaRecordStore
        .getRecordDefinition(SiteTables.SITE_POINT);
      try (
        final RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file)) {
        for (final Record duplicateIdentifierRecord : duplicateIdentifierRecords) {
          writer.write(duplicateIdentifierRecord);
        }
        Logs.warn(this, "Check log " + fileName);
      }
    }
  }

  private void logEmergencyManagementSiteMissingNames() {
    final String fileName = "em_site_no_structured_name_match.xlsx";
    final Path file = ImportSites.SITES_DIRECTORY.resolve(fileName);
    if (this.emMissingNameBySiteId.isEmpty()) {
      Paths.deleteDirectories(file);
    } else {
      final RecordDefinition recordDefinition = new RecordDefinitionBuilder("ERROR")//
        .addField("SITE_ID", DataTypes.INT) //
        .addField("NAME", DataTypes.STRING, 50) //
        .getRecordDefinition();
      try (
        final RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file)) {
        for (final Entry<Identifier, String> entry : this.emMissingNameBySiteId.entrySet()) {
          final Identifier siteId = entry.getKey();
          final String name = entry.getValue();
          writer.write(siteId, name);
        }
        Logs.warn(this, "Check log " + fileName);
      }
    }
  }

  private void logEmergencyManagementSiteWrongSiteType() {
    final String fileName = "em_site_wrong_site_type.xlsx";
    final Path file = ImportSites.SITES_DIRECTORY.resolve(fileName);
    if (this.emWrongSiteType.isEmpty()) {
      Paths.deleteDirectories(file);
    } else {
      final RecordDefinition recordDefinition = new RecordDefinitionBuilder("ERROR")//
        .addField("SITE_ID", DataTypes.INT) //
        .addField("PL_GROUP", DataTypes.STRING, 30) //
        .addField("PL_TYPE", DataTypes.STRING, 30) //
        .getRecordDefinition();
      try (
        final RecordWriter writer = RecordWriter.newRecordWriter(recordDefinition, file)) {
        for (final Object[] record : this.emWrongSiteType) {
          writer.write(record);
        }
        Logs.warn(this, "Check log " + fileName);
      }
    }
  }

  public void run() {
    this.dialog.setSelectedTab(EM_SITES);
    loadEmergencyManagementSites();
    System.out.println(this.typeCounts.toTsv());
  }

}
