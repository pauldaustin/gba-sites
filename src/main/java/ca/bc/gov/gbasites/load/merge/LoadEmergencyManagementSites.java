package ca.bc.gov.gbasites.load.merge;

import java.nio.file.Path;
import java.util.ArrayList;
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
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.model.type.code.CommunityPoly;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;
import ca.bc.gov.gbasites.model.type.code.SiteLocationCode;

import com.revolsys.collection.map.CollectionMap;
import com.revolsys.geometry.model.Point;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.io.Reader;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Counter;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.count.LabelCountMap;
import com.revolsys.util.count.LabelCounters;

public class LoadEmergencyManagementSites implements SitePoint {

  private static final Path EM_SITES_DIRECTORY = ImportSites.SITES_DIRECTORY
    .resolve("EmergencyManagement");

  public static final String IGNORE_XCOVER = "Ignore XCOVER";

  public static final String EM_SITES = "Em Sites";

  private final BoundaryCache communityCache = CommunityPoly.getCommunities();

  private final BoundaryCache localityCache = GbaController.getLocalities();

  private final BoundaryCache regionalDistrictCache = GbaController.getRegionalDistricts();

  private final Map<Identifier, String> emMissingNameBySiteId = Identifier.newTreeMap();

  private final List<Object[]> emSiteDifferentCityFromLocalities = new ArrayList<>();

  private final List<Object[]> emWrongSiteType = new ArrayList<>();

  private final LabelCounters typeCounts = new LabelCountMap();

  private final RecordDefinition recordDefinition = ImportSites.getSitePointTsvRecordDefinition();

  private final CodeTable sitePointTypes = GbaController.getCodeTable(SiteTables.SITE_TYPE_CODE);

  private final CollectionMap<String, Record, List<Record>> sitesByLocality = CollectionMap
    .hashArray();

  public LoadEmergencyManagementSites() {
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

  private Record loadEmergencyManagementSite(final Record emSite) {
    final Identifier id = emSite.getIdentifier("ID");
    final String plType = Strings.trim(emSite.getString("PL_TYPE"));
    if ("XCOVER".equals(plType)) {
      throw IgnoreSiteException.warning(IGNORE_XCOVER);
    } else {
      final String plGroup = Strings.trim(emSite.getString("PL_GROUP"));
      this.typeCounts.addCount(plGroup + " " + plType);
      String siteName1 = Strings.trim(emSite.getString("PL_NAME"));
      String civicNumber = Strings.trim(emSite.getString("ADDR_NUM"));
      String name = Strings.trim(emSite.getString("ADDR_ROAD"));
      final String addrOther = Strings.trim(emSite.getString("ADDR_OTHER"));
      final String city = Strings.trim(emSite.getString("CITY"));
      @SuppressWarnings("unused")
      final String MAP_CHAR = Strings.trim(emSite.getString("MAP_CHAR"));
      final String siteName2 = Strings.trim(emSite.getString("PL_ALIAS"));
      final String siteName3 = Strings.trim(emSite.getString("PL_ALIAS2"));
      Point point = emSite.getGeometry();
      final double x = point.getX();
      final double y = point.getY();
      point = Gba.GEOMETRY_FACTORY_2D_1M.point(x, y);

      final Record site = this.recordDefinition.newRecord();

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

      final Identifier localityId = this.localityCache.setBoundaryIdAndName(site, point,
        LOCALITY_NAME);
      final String localityName = this.localityCache.getValue(localityId);
      this.sitesByLocality.addValue(localityName, site);

      this.communityCache.setBoundaryIdAndName(site, point, COMMUNITY_NAME);
      this.regionalDistrictCache.setBoundaryIdAndName(site, point, REGIONAL_DISTRICT_NAME);

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

      site.setValue(SITE_ID, id);
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
          fullAddress = localityName + " " + this.sitePointTypes.getValue(siteTypeCode);
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
      site.setValue(CREATE_PARTNER_ORG, "GeoBC");
      site.setValue(MODIFY_PARTNER_ORG, "GeoBC");
      site.setValue(CUSTODIAN_PARTNER_ORG, "GeoBC");
      site.setValue(OPEN_DATA_IND, "N");

      if (localityId == null) {
        extendedData.put("CITY", city);
        this.emSiteDifferentCityFromLocalities.add(new Object[] {
          id.toString(), fullAddress, city, null
        });
      } else {
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

  public CollectionMap<String, Record, List<Record>> loadEmergencyManagementSites(
    final StatisticsDialog dialog) {
    Paths.createDirectories(EM_SITES_DIRECTORY);
    final Counter readCounter = dialog.getCounter(EM_SITES, SiteTables.SITE_POINT,
      BatchUpdateDialog.READ);
    final Counter writeCounter = dialog.getCounter(EM_SITES, SiteTables.SITE_POINT,
      BatchUpdateDialog.WRITE);
    final Counter xcoverCounter = dialog.getCounter(EM_SITES, SiteTables.SITE_POINT, IGNORE_XCOVER);

    try (
      RecordStore emRecordStore = FileGdbRecordStoreFactory
        .newRecordStore(GbaController.getAncillaryDirectory("sites.gdb"))) {
      emRecordStore.initialize();
      final Query query = new Query("/sites");
      query.setCancellable(dialog);
      query.addOrderBy("ID", true);
      try (
        Reader<Record> emReader = emRecordStore.getRecords(query)) {
        for (final Record emSite : dialog.cancellable(emReader)) {
          readCounter.add();
          try {
            loadEmergencyManagementSite(emSite);
            writeCounter.add();
          } catch (final IgnoreSiteException e) {
            xcoverCounter.add();
          }
        }
      }
    }
    logEmergencyManagementSiteMissingNames();
    logEmergencyManagementSiteDifferentLocalities();
    logEmergencyManagementSiteWrongSiteType();
    return this.sitesByLocality;
  }

  private void logEmergencyManagementSiteDifferentLocalities() {
    final String fileName = "em_site_city_locality_different.xlsx";
    final Path file = EM_SITES_DIRECTORY.resolve(fileName);
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

  private void logEmergencyManagementSiteMissingNames() {
    final String fileName = "em_site_no_structured_name_match.xlsx";
    final Path file = EM_SITES_DIRECTORY.resolve(fileName);
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
    final Path file = EM_SITES_DIRECTORY.resolve(fileName);
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

}
