package ca.bc.gov.gbasites.load.common.converter;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.type.code.NameDirection;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.common.StructuredNameMapping;
import ca.bc.gov.gbasites.load.provider.other.ImportSites;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.code.CommunityPoly;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.beans.Classes;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapSerializer;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.Uuid;

public abstract class AbstractSiteConverter extends BaseObjectWithProperties
  implements SitePoint, MapSerializer {

  public static final StructuredNames STRUCTURED_NAMES = GbaController.getStructuredNames();

  private static final Map<String, Set<String>> nameSuffixCodeByAlias = new HashMap<>();

  public static final String IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS = "Ignored FULL_ADDRESS != parts";

  private static final CodeTable SUFFIX_CODE_TABLE = STRUCTURED_NAMES.getSuffixCodeTable();

  private static final Set<String> IGNORE_NAME_ERRORS = Sets.newHash("Name suffix was an alias",
    "SUFFIX_NAME_DIRECTION_CODE was full direction not code",
    "PREFIX_NAME_DIRECTION_CODE was full direction not code");

  private final static Map<Identifier, Map<String, Set<String>>> nameDifferentByPartnerOrganizationIdAndLocality = new HashMap<>();

  public final static Map<Identifier, RecordWriter> nameDifferentWriterByPartnerOrganizationId = new HashMap<>();

  protected static final Map<String, Map<String, FeatureStatus>> featureStatusByLocalityAndStreetName = new HashMap<>();

  private static final Map<String, Map<String, Identifier>> structuredNameIdByCustodianAndAliasName = new HashMap<>();

  private static final Map<String, Map<String, String>> structuredNameByCustodianAndAliasName = new HashMap<>();

  public static final String FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR = "FULL_ADDRESS has extra UNIT_DESCRIPTOR";

  public static final String IGNORE_STREET_NAME_NOT_SPECIFIED = "Ignore STREET_NAME not specified";

  public static final BoundaryCache regionalDistricts = GbaController.getRegionalDistricts();

  public static void init() {
    loadStructuredNameIdByCustodianAndAliasName();
  }

  protected static void loadStructuredNameIdByCustodianAndAliasName() {
    synchronized (structuredNameIdByCustodianAndAliasName) {
      if (structuredNameIdByCustodianAndAliasName.isEmpty()) {
        final Path file = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
          .resolve("STRUCTURED_NAME_ALIAS.xlsx");
        if (Paths.exists(file)) {
          try (
            RecordReader reader = RecordReader.newRecordReader(file)) {
            for (final Record record : reader) {
              final String dataProvider = record.getString("DATA_PROVIDER");
              final String nameAlias = record.getString("NAME_ALIAS");
              final String structuredName = record.getString("STRUCTURED_NAME");
              final Identifier structuredNameId = GbaController
                .getCodeTableIdentifier(GbaTables.STRUCTURED_NAME, structuredName);
              if (structuredNameId == null) {
                if ("Y".equals(record.getString("CREATE_STRUCTURED_NAME"))) {
                  continue;
                  // structuredNameId =
                  // STRUCTURED_NAMES.newStructuredNameId(structuredName);
                }
              }
              final FeatureStatus featureStatus = FeatureStatus.getFeatureStatus(record);
              if (!featureStatus.isActive()) {
                Maps.put(featureStatusByLocalityAndStreetName, dataProvider, nameAlias,
                  featureStatus);
              }
              if (Property.hasValuesAll(dataProvider, nameAlias, structuredNameId)) {
                Maps.put(structuredNameByCustodianAndAliasName, dataProvider, nameAlias,
                  structuredName);
                Maps.put(structuredNameIdByCustodianAndAliasName, dataProvider, nameAlias,
                  structuredNameId);
              } else if (!featureStatus.isIgnored()) {
                Logs.error(AbstractSiteConverter.class,
                  "Invalid mapping in " + file + "\n" + record);
              }
            }
          }
        }
      }
    }
  }

  protected Set<String> ignoreNames = Sets.newHash("PARK", "N/A", "LANE ACCESS", "LANE ALLOWANCE",
    "NO NAME AV", "NON-AVENUE", "NON-STREET", "RIGHT-OF-WAY", "PRIVITE RD");

  private String idFieldName;

  private String addressFieldName;

  public void addError(final Record record, final String message) {
    ProviderSitePointConverter.addError(message);
  }

  protected void addStructuredNameError(final String dataProvider, final String message,
    final String originalName, final String structuredName, final Identifier structuredNameId) {
    final Identifier partnerOrganizationId = getPartnerOrganizationId();
    String localityName = ProviderSitePointConverter.getLocalityName();
    Map<String, Set<String>> nameDifferentByLocality = nameDifferentByPartnerOrganizationIdAndLocality
      .get(partnerOrganizationId);
    if (nameDifferentByLocality == null) {
      nameDifferentByLocality = new TreeMap<>();
      nameDifferentByPartnerOrganizationIdAndLocality.put(partnerOrganizationId,
        nameDifferentByLocality);
    }
    if (!IGNORE_NAME_ERRORS.contains(message)) {
      if (localityName == null) {
        localityName = "Unknown";
      }
      if (Maps.addToSet(nameDifferentByLocality, localityName, originalName)) {
        RecordWriter writer;
        synchronized (nameDifferentWriterByPartnerOrganizationId) {
          writer = nameDifferentWriterByPartnerOrganizationId.get(partnerOrganizationId);
          if (writer == null) {
            final Path path = ProviderSitePointConverter.getDataProviderFile(partnerOrganizationId,
              "_NAME_ERROR" + ".xlsx", "_");
            final RecordDefinition nameErrorRecordDefinition = new RecordDefinitionBuilder(
              Paths.getBaseName(path))//
                .addField("DATA_PROVIDER", DataTypes.STRING, dataProvider.length()) //
                .addField("LOCALITY", DataTypes.STRING, 30) //
                .addField("STREET_NAME", DataTypes.STRING, 45) //
                .addField("STRUCTURED_NAME", DataTypes.STRING, 45) //
                .addField("STRUCTURED_NAME_ID", DataTypes.INT, 10) //
                .addField("MESSAGE", DataTypes.STRING, 60) //
                .getRecordDefinition();
            writer = RecordWriter.newRecordWriter(nameErrorRecordDefinition, path);
            nameDifferentWriterByPartnerOrganizationId.put(partnerOrganizationId, writer);
          }
        }
        writer.write(dataProvider, localityName, originalName, structuredName, structuredNameId,
          message);
        getDialog().addLabelCount(ProviderSitePointConverter.NAME_ERRORS, message,
          StatisticsDialog.ERROR);
      }
    }
  }

  public void addWarningCount(final String message) {
    ProviderSitePointConverter.addWarningCount(message);
  }

  public abstract SitePointProviderRecord convert(Record sourceRecord, Point sourcePoint);

  private void fixStructuredName(final String dataProvider, final Record sourceRecord,
    final String structuredName, final String originalName,
    final StructuredNameMapping structuredNameMapping) {
    if (structuredNameMapping.matchReplace("Replaced smart quote", true, "`", "'")) {
      return;
    }
    {
      String streetName = structuredNameMapping.getCurrentStructuredName();
      int spaceIndex = streetName.lastIndexOf(' ');
      if (spaceIndex != -1) {
        String nameSuffixCode = null;
        final String nameDirectionAlias = streetName.substring(spaceIndex + 1);
        final NameDirection nameDirectionCode = NameDirection
          .getDirection(nameDirectionAlias.replace("(", "").replace(")", ""));
        if (nameDirectionCode != null) {
          streetName = streetName.substring(0, spaceIndex);
          String message;
          if (nameDirectionAlias.contains("(")) {
            message = "SUFFIX_NAME_DIRECTION_CODE contains '('";
          } else if (nameDirectionAlias.contains("(")) {
            message = "SUFFIX_NAME_DIRECTION_CODE contains ')'";
          } else if (!nameDirectionCode.getCode().equals(nameDirectionAlias)) {
            message = "SUFFIX_NAME_DIRECTION_CODE was full direction not code";
          } else {
            message = null;
          }
          if (message != null && structuredNameMapping.setStructuredName(message, true, streetName,
            nameDirectionCode)) {
            return;
          }
        }

        spaceIndex = streetName.lastIndexOf(' ');
        if (spaceIndex != -1) {
          final String nameSuffix = streetName.substring(spaceIndex + 1);
          nameSuffixCode = SUFFIX_CODE_TABLE.getValue(nameSuffix);
          if (nameSuffixCode == null) {
            final Set<String> aliases = getNameSuffixCodeByAlias(nameSuffix);
            for (final String suffixAlias : aliases) {
              streetName = streetName.substring(0, spaceIndex);
              final boolean forceSetCode = aliases.size() == 1;
              if (structuredNameMapping.setStructuredName("Name suffix was an alias", forceSetCode,
                streetName, suffixAlias, nameDirectionCode)) {
                return;
              }
            }
          } else {
            nameSuffixCode = nameSuffixCode.toUpperCase();
            streetName = streetName.substring(0, spaceIndex);
            if (structuredNameMapping.setStructuredName("Name suffix was an alias", true,
              streetName, nameSuffixCode, nameDirectionCode)) {
              return;
            }
          }
        }
      }
    }
    if (structuredNameMapping.contains("'")) {
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, "'",
        "")) {
        return;
      }
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, ".",
        "")) {
        return;
      }
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, "'",
        "-")) {
        return;
      }
    }
    if (structuredNameMapping.contains(".")) {
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, ".",
        "")) {
        return;
      }
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, ".",
        " ")) {
        return;
      }
    }
    if (structuredNameMapping.contains("-")) {
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, "-",
        " ")) {
        return;
      }
      if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false, "-",
        "")) {
        return;
      }
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different punctuation", false,
      "TRANS CANADA", "TRANS-CANADA")) {
      return;
    }
    if (structuredNameMapping.matchReplaceAll("STREET_NAME has different punctuation", false,
      "^HIGHWAY", "HWY")) {
      return;
    }

    if (structuredNameMapping.startsWith("NO ") && structuredNameMapping.endsWith(" HWY")) {
      final String name = structuredNameMapping.getCurrentStructuredName();
      final String highwayName = "Hwy " + name.substring(3, name.length() - 4);
      if (structuredNameMapping.setStructuredName("No X Hwy", true, highwayName)) {
        return;
      }
    }
    if (structuredNameMapping.endsWith(" HWY")) {
      String highwayName = structuredNameMapping.getCurrentStructuredName();
      final String firstPart = Strings.firstPart(highwayName, ' ');
      final NameDirection nameDirection = NameDirection.getDirection(firstPart);
      if (nameDirection == null) {
        highwayName = "Hwy " + highwayName.substring(0, highwayName.length() - 4);
      } else {
        highwayName = "Hwy "
          + highwayName.substring(firstPart.length() + 1, highwayName.length() - 4) + " "
          + nameDirection;
      }
      if (structuredNameMapping.setStructuredName("X Hwy", true, highwayName)) {
        return;
      }
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", true, "HIGHWAY",
      "HWY")) {
      return;
    }
    if (structuredNameMapping.startsWith("ST ") || structuredNameMapping.contains(" ST ")) {
      if (fixStructuredNameSaint(dataProvider, sourceRecord, structuredNameMapping,
        structuredNameMapping.getCurrentStructuredName())) {
        return;
      }
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " FSR",
      " ROAD FSR")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " RD",
      " FSR")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false,
      " FRONTAGE RD", " RD")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false,
      " FRONTAGE RD", " FRTG")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " ACC",
      " ACCESS")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " ACC",
      " ACCESS RD")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " ACCESS",
      " ACCESS RD")) {
      return;
    }
    if (structuredNameMapping.matchReplaceAll("STREET_NAME has extra in ()", false, " \\(.+\\)",
      "")) {
      return;
    }
    if (structuredNameMapping.matchSuffix("STREET_NAME doesn't have suffix used by STRUCTURED_NAME",
      false, " RD")) {
      return;
    }
    for (final String direction : Arrays.asList(" N", " E", " S", " W", " NE", " NW", " SE",
      " SW")) {
      if (structuredNameMapping.matchReplace(
        "STREET_NAME has SUFFIX_NAME_DIRECTION_CODE not in STRUCTURED_NAME", false, direction,
        "")) {
        return;
      }
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has extra suffix not in STRUCTURED_NAME",
      false, " RD", "")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has extra suffix not in STRUCTURED_NAME",
      false, " ROAD", " RD")) {
      return;
    }
    if (structuredNameMapping.matchReplace("STREET_NAME has different spelling", false, " CK",
      " CREEK")) {
      return;
    }
    {
      final String cleanedStreetName = structuredNameMapping.getCurrentStructuredName();
      final String firstPart = Strings.firstPart(cleanedStreetName, ' ');
      final NameDirection nameDirection = NameDirection.getDirection(firstPart);
      if (nameDirection != null) {
        final String nameWithoutDirection = cleanedStreetName.substring(firstPart.length() + 1);
        final String dirPrefixName = nameDirection + " " + nameWithoutDirection;
        if (structuredNameMapping.setStructuredName(
          "PREFIX_NAME_DIRECTION_CODE was full direction not code", false, dirPrefixName)) {
          return;
        }
        if (fixStructuredNameSaint(dataProvider, sourceRecord, structuredNameMapping,
          dirPrefixName)) {
          return;
        }
        final String dirSuffixName = nameWithoutDirection + " " + nameDirection;
        if (structuredNameMapping.setStructuredName(
          "STREET_NAME has direction as suffix instead of prefix", false, dirSuffixName)) {
          return;
        }
      }
    }
    {
      final String cleanedStreetName = structuredNameMapping.getCurrentStructuredName();
      final String lastPart = Strings.lastPart(cleanedStreetName, ' ');
      final NameDirection nameDirection = NameDirection.getDirection(lastPart);
      final String nameWithoutDirection = cleanedStreetName.substring(0,
        cleanedStreetName.length() - lastPart.length() - 1);
      final String dirSuffixName = nameWithoutDirection + " " + nameDirection;
      if (structuredNameMapping.setStructuredName(
        "SUFFIX_NAME_DIRECTION_CODE was full direction not code", false, dirSuffixName)) {
        return;
      }
      if (fixStructuredNameSaint(dataProvider, sourceRecord, structuredNameMapping,
        dirSuffixName)) {
        return;
      }
      final String dirPrefixName = nameDirection + " " + nameWithoutDirection;
      if (structuredNameMapping.setStructuredName(
        "STREET_NAME has direction as prefix instead of suffix", false, dirPrefixName)) {
        return;
      }
    }
    if (structuredNameMapping.matchReplaceFirst("STREET_NAME uses plural 'S", false, "S ", "'S ")) {
      return;
    }
    if (structuredNameMapping.setUsingSimplifiedName()) {
      return;
    }
  }

  private boolean fixStructuredNameSaint(final String dataProvider, final Record sourceRecord,
    final StructuredNameMapping structuredNameMapping, String name) {
    name = name.replaceFirst("ST ", "ST. ");
    if (structuredNameMapping.setStructuredName("STREET_NAME uses ST. for ST SAINT name", false,
      name)) {
      return true;
    }

    name = name.replaceFirst("S ", "'S ");
    return structuredNameMapping.setStructuredName("STREET_NAME uses plural 'S", false, name);
  }

  public String getAddressFieldName() {
    return this.addressFieldName;
  }

  protected StatisticsDialog getDialog() {
    return ImportSites.dialog;
  }

  public String getIdFieldName() {
    return this.idFieldName;
  }

  public Set<String> getNameSuffixCodeByAlias(final String nameSuffixAlias) {
    synchronized (nameSuffixCodeByAlias) {
      if (nameSuffixCodeByAlias.isEmpty()) {
        final Path nameSuffixAliasFile = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
          .resolve("NAME_SUFFIX_CODE_ALIAS.xlsx");
        if (Paths.exists(nameSuffixAliasFile)) {
          try (
            RecordReader reader = RecordReader.newRecordReader(nameSuffixAliasFile)) {
            for (final Record record : reader) {
              final String alias = record.getString("ALIAS");
              final String code = record.getString("NAME_SUFFIX_CODE");
              Maps.addToSet(nameSuffixCodeByAlias, alias.toUpperCase(), code.toUpperCase());
            }
          }
        }
      }
    }
    final Set<String> aliases = nameSuffixCodeByAlias.get(nameSuffixAlias.toUpperCase());
    if (aliases == null) {
      return Collections.emptySet();
    } else {
      return aliases;
    }
  }

  protected Identifier getPartnerOrganizationId() {
    return ProviderSitePointConverter.getPartnerOrganizationIdThread();
  }

  protected String getPartnerOrganizationShortName() {
    return ProviderSitePointConverter.getPartnerOrganizationShortName();
  }

  public String getStructuredNameFromAlias(final String dataProvider, final String nameAlias) {
    return Maps.getMap(structuredNameByCustodianAndAliasName, dataProvider, nameAlias);
  }

  public final String getType() {
    return "gba" + Classes.className(this);
  }

  public String getUpperString(final Record record, final String fieldName) {
    final String string = Strings.cleanWhitespace(record.getString(fieldName));
    if (Property.hasValue(string)) {
      final String value = Strings.upperCase(string);
      if (!"<NULL>".equals(value)) {
        return value;
      }
    }
    return "";
  }

  public SitePointProviderRecord newSitePoint(final Point point) {
    final RecordDefinitionImpl recordDefinition = ProviderSitePointConverter
      .getSitePointTsvRecordDefinition();
    final SitePointProviderRecord sitePoint = new SitePointProviderRecord(recordDefinition);
    sitePoint.setGeometryValue(point);

    final Identifier regionalDistrictId = regionalDistricts.getBoundaryId(point);
    sitePoint.setValue(REGIONAL_DISTRICT_ID, regionalDistrictId);

    final Identifier communityId = CommunityPoly.getCommunities().getBoundaryId(point);
    sitePoint.setValue(COMMUNITY_ID, communityId);

    return sitePoint;
  }

  public void setAddressFieldName(final String addressFieldName) {
    this.addressFieldName = addressFieldName;
  }

  protected void setCustodianSiteId(final SitePointProviderRecord sitePoint,
    final Record sourceRecord) {
    final String idFieldName = getIdFieldName();
    final String custodianSiteId;
    if (idFieldName == null) {
      final StringBuilder uuid = new StringBuilder();
      final Point point = sitePoint.getGeometry();
      final double xId = point.getX();
      uuid.append(xId);
      uuid.append(',');

      final double yId = point.getY();
      uuid.append(yId);
      uuid.append(',');
      final String fullAddress = sitePoint.getFullAddress();
      uuid.append(fullAddress);

      custodianSiteId = Uuid.sha1("addressPoint", uuid).toString();
    } else {
      custodianSiteId = sourceRecord.getString(idFieldName);
    }
    sitePoint.setValue(CUSTODIAN_SITE_ID, custodianSiteId);
  }

  public void setIdFieldName(final String idFieldName) {
    this.idFieldName = idFieldName;
  }

  public boolean setStructuredName(final Record sourceRecord, final Record sitePoint,
    final int nameIndex, String name, final String originalName) {
    final String partnerOrganizationShortName = getPartnerOrganizationShortName();
    if (Property.isEmpty(name) || this.ignoreNames.contains(originalName)) {
      return false;
    } else {
      loadStructuredNameIdByCustodianAndAliasName();
      final StructuredNameMapping structuredNameMapping = new StructuredNameMapping(originalName,
        name);
      final FeatureStatus featureStatusCode = Maps.getMap(featureStatusByLocalityAndStreetName,
        partnerOrganizationShortName.toUpperCase(), originalName, FeatureStatus.ACTIVE);
      if (featureStatusCode.isIgnored()) {
        final String message = "Ignored STREET_NAME in STRUCTURED_NAME_ALIAS.xlsx";
        addStructuredNameError(partnerOrganizationShortName, message, originalName, null, null);

        throw new IgnoreSiteException(message);
      } else if (sitePoint.equalValue(FEATURE_STATUS_CODE, "A")) {
        sitePoint.setValue(FEATURE_STATUS_CODE, featureStatusCode.getCode());
      }
      if (structuredNameMapping.isNotMatched()) {
        name = Strings.cleanWhitespace(name);
        final Identifier mappedStructuredNameId = Maps.getMap(
          structuredNameIdByCustodianAndAliasName, Strings.upperCase(partnerOrganizationShortName),
          name);
        if (!structuredNameMapping.setStructuredNameId(mappedStructuredNameId,
          "Mapped from STRUCTURED_NAME_ALIAS.xlsx")) {
          fixStructuredName(partnerOrganizationShortName, sourceRecord, name, originalName,
            structuredNameMapping);
        }

        final String message = structuredNameMapping.getMessage();
        final String structuredName = structuredNameMapping.getMatchedStructuredName();
        final Identifier structuredNameId = structuredNameMapping.getStructuredNameId();
        addStructuredNameError(partnerOrganizationShortName, message, originalName, structuredName,
          structuredNameId);
      }

      final String idFieldName;
      final String nameFieldName;
      if (nameIndex == 0) {
        idFieldName = STREET_NAME_ID;
        nameFieldName = "STREET_NAME";
      } else {
        nameFieldName = "STREET_NAME_ALIAS" + nameIndex;
        idFieldName = nameFieldName + "_ID";
      }
      if (structuredNameMapping.isMatched()) {

        final String structuredName = structuredNameMapping.getCurrentStructuredName();
        sitePoint.setValue(nameFieldName, structuredName);
        sitePoint.setValue(idFieldName, structuredNameMapping.getStructuredNameId());
        return true;
      } else {
        sitePoint.setValue(nameFieldName, name);
        addError(sourceRecord, "STRUCTURED_NAME not found");
        return true;
      }
    }

  }

  @Override
  public MapEx toMap() {
    final String type = getType();
    final MapEx map = newTypeMap(type);
    addToMap(map, "idFieldName", this.idFieldName);
    addToMap(map, "addressFieldName", this.addressFieldName);
    return map;
  }
}
