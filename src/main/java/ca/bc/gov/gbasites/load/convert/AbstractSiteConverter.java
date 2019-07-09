package ca.bc.gov.gbasites.load.convert;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.jeometry.common.compare.CompareUtil;
import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.model.type.code.NameDirection;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.StructuredNames;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.IgnoreSiteException;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.common.SitePointProviderRecord;
import ca.bc.gov.gbasites.load.common.StructuredNameMapping;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.code.CommunityPoly;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.beans.Classes;
import com.revolsys.collection.map.CollectionMap;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygonal;
import com.revolsys.geometry.operation.union.UnaryUnionOp;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;
import com.revolsys.util.Uuid;

public abstract class AbstractSiteConverter extends AbstractRecordConverter<SitePointProviderRecord>
  implements SitePoint {

  public static final StructuredNames STRUCTURED_NAMES = GbaController.getStructuredNames();

  private static final Map<String, Set<String>> nameSuffixCodeByAlias = new HashMap<>();

  public static final String IGNORED_FULL_ADDRESS_NOT_EQUAL_PARTS = "Ignored FULL_ADDRESS != parts";

  private static final CodeTable SUFFIX_CODE_TABLE = STRUCTURED_NAMES.getSuffixCodeTable();

  private static final Set<String> IGNORE_NAME_ERRORS = Sets.newHash("Name suffix was an alias",
    "SUFFIX_NAME_DIRECTION_CODE was full direction not code",
    "PREFIX_NAME_DIRECTION_CODE was full direction not code");

  protected static final Map<String, Map<String, FeatureStatus>> featureStatusByLocalityAndStreetName = new HashMap<>();

  private static final Map<String, Map<String, Identifier>> structuredNameIdByCustodianAndAliasName = new HashMap<>();

  private static final Map<String, Map<String, String>> structuredNameByCustodianAndAliasName = new HashMap<>();

  public static final String FULL_ADDRESS_HAS_EXTRA_UNIT_DESCRIPTOR = "FULL_ADDRESS has extra UNIT_DESCRIPTOR";

  public static final String IGNORE_STREET_NAME_NOT_SPECIFIED = "Ignore STREET_NAME not specified";

  public static final BoundaryCache regionalDistricts = GbaController.getRegionalDistricts();

  public static RecordDefinitionImpl sitePointTsvRecordDefinition;

  private static final RecordDefinitionImpl RECORD_DEFINITION = ImportSites
    .getSitePointTsvRecordDefinition();

  private static final BoundaryCache localities = GbaController.getLocalities();

  private static final BoundaryCache communities = CommunityPoly.getCommunities();

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

  private RecordWriter nameDifferentWriter;

  protected Set<String> ignoreNames = Sets.newHash("PARK", "N/A", "LANE ACCESS", "LANE ALLOWANCE",
    "NO NAME AV", "NON-AVENUE", "NON-STREET", "RIGHT-OF-WAY", "PRIVITE RD");

  private String idFieldName;

  private String addressFieldName;

  private ProviderSitePointConverter providerConfig;

  protected AtomicPathUpdator pathUpdator;

  private AtomicPathUpdator nameDifferentPathUpdator;

  private final Map<String, Set<String>> nameDifferentByLocality = new TreeMap<>();

  // private final Map<String, List<Record>> recordsByLocalityName = new
  // TreeMap<>();
  private final CollectionMap<String, Record, List<Record>> recordsByLocalityName = CollectionMap
    .treeArray();

  private final Map<String, List<Geometry>> sourceGeometryByLocality = new TreeMap<>();

  private String openDataInd = "N";

  public void addProviderBoundary(final Map<String, List<Geometry>> sourceGeometryByLocality) {
    if (ProviderSitePointConverter.calculateBoundary) {
      for (final Entry<String, List<Geometry>> entry : sourceGeometryByLocality.entrySet()) {
        final String localityName = entry.getKey();
        final List<Geometry> geometries = entry.getValue();
        // int totalArea = 0;
        // for (final Geometry geometry : geometries) {
        // totalArea += geometry.getArea();
        // }
        Geometry boundary = UnaryUnionOp.union(geometries);
        if (boundary == null) {
          Debug.noOp();
        } else {
          if (boundary instanceof Polygonal) {
            // final double area = boundary.getArea();
            // if (Math.abs(totalArea - area) > 100) {
            // Debug.noOp();
            // }
            // for (final Geometry geometry : geometries) {
            // if (!boundary.contains(geometry.getPointWithin())) {
            // Debug.noOp();
            // }
            // }
          } else {
            boundary = boundary.convexHull();
          }
          Maps.addToMap(Maps.factoryTree(),
            ProviderSitePointConverter.boundaryByProviderAndLocality, getPartnerOrganizationName(),
            localityName, boundary);
        }
      }
    }

  }

  protected void addStructuredNameError(final String message, final String originalName,
    final String structuredName, final Identifier structuredNameId) {
    final String dataProvider = getPartnerOrganizationShortName();

    if (!IGNORE_NAME_ERRORS.contains(message)) {
      if (this.localityName == null) {
        this.localityName = "Unknown";
      }
      if (Maps.addToSet(this.nameDifferentByLocality, this.localityName, originalName)) {
        if (this.nameDifferentWriter == null) {
          this.nameDifferentPathUpdator = this.partnerOrganizationFiles
            .newPathUpdator(ImportSites.NAME_ERROR_BY_PROVIDER);
          final Path path = this.nameDifferentPathUpdator.getPath();
          final RecordDefinition nameErrorRecordDefinition = new RecordDefinitionBuilder(
            Paths.getBaseName(path))//
              .addField("DATA_PROVIDER", DataTypes.STRING, dataProvider.length()) //
              .addField("LOCALITY", DataTypes.STRING, 30) //
              .addField("STREET_NAME", DataTypes.STRING, 45) //
              .addField("STRUCTURED_NAME", DataTypes.STRING, 45) //
              .addField("STRUCTURED_NAME_ID", DataTypes.INT, 10) //
              .addField("MESSAGE", DataTypes.STRING, 60) //
              .getRecordDefinition();
          this.nameDifferentWriter = RecordWriter.newRecordWriter(nameErrorRecordDefinition, path);
        }
        this.nameDifferentWriter.write(dataProvider, this.localityName, originalName,
          structuredName, structuredNameId, message);
        getDialog().addLabelCount(ProviderSitePointConverter.NAME_ERRORS, message,
          StatisticsDialog.ERROR);
      }
    }
  }

  private void addWithCivicNumber(final SitePointProviderRecord sitePoint, final int civicNumber) {
    final SitePointProviderRecord record = sitePoint.clone();
    record.setCivicNumber(civicNumber);
    record.setCivicNumberRange(null);
    record.updateFullAddress();
    this.recordsByLocalityName.addValue(this.localityName, record);
  }

  @Override
  public void close() {
    super.close();
    this.nameDifferentByLocality.clear();
    if (this.nameDifferentWriter != null) {
      this.nameDifferentWriter.close();
      this.nameDifferentPathUpdator.close();
      this.nameDifferentWriter = null;
      this.nameDifferentPathUpdator = null;
    }
  }

  @Override
  protected SitePointProviderRecord convertRecordDo(final Record sourceRecord) {
    final Geometry sourceGeometry = sourceRecord.getGeometry();
    if (Property.isEmpty(sourceGeometry)) {
      throw IgnoreSiteException.warning("Ignore Record does not contain a point geometry");
    } else {
      final Geometry convertedSourceGeometry = getValidSourceGeometry(sourceGeometry);

      final Point point = convertedSourceGeometry.getPointWithin();

      this.localityId = localities.getBoundaryId(point);
      if (this.localityId == null) {
        this.localityName = "Unknown";
      } else {
        this.localityName = localities.getValue(this.localityId);
      }
      if (Property.isEmpty(point) || !point.isValid()) {
        throw IgnoreSiteException.warning("Ignore Record does not contain a point geometry");
      } else {
        if (this.localityId != null) {
          if (ProviderSitePointConverter.isCalculateBoundary()) {
            Maps.addToList(this.sourceGeometryByLocality, this.localityName,
              convertedSourceGeometry);
          }
        }
        final SitePointProviderRecord sitePoint = convertRecordSite(sourceRecord, point);
        if (sitePoint == null) {
          return null;
        } else {
          sitePoint.setValue(OPEN_DATA_IND, this.openDataInd);
          sitePoint.setValue(LOCALITY_ID, this.localityId);
          sitePoint.setValue(LOCALITY_NAME, this.localityName);
          if (sitePoint.equalValue(UNIT_DESCRIPTOR, "0")) {
            sitePoint.setValue(UNIT_DESCRIPTOR, null);
          }
          sitePoint.updateFullAddress();
          sitePoint.setCreateModifyOrg(this.createModifyPartnerOrganization);
          sitePoint.setCustodianOrg(getPartnerOrganization());
          return sitePoint;
        }
      }
    }
  }

  public abstract SitePointProviderRecord convertRecordSite(Record sourceRecord, Point sourcePoint);

  @Override
  public void convertSourceRecords() {
    try (
      AtomicPathUpdator pathUpdator = this.partnerOrganizationFiles
        .newPathUpdator(ImportSites.SITE_POINT_BY_PROVIDER)) {
      setPathUpdator(pathUpdator);
      super.convertSourceRecords();
    }
  }

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

  public List<Record> getRecordsForLocality(final String localityName) {
    return this.recordsByLocalityName.getOrEmpty(localityName);
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

  protected Geometry getValidSourceGeometry(final Geometry sourceGeometry) {
    if (sourceGeometry.isGeometryCollection()) {
      final List<Geometry> geometries = new ArrayList<>();
      for (final Geometry geometry : sourceGeometry.geometries()) {
        final Geometry convertGeometry = geometry.convertGeometry(Gba.GEOMETRY_FACTORY_2D);
        geometries.add(convertGeometry);
      }
      return UnaryUnionOp.union(geometries);
    } else {
      final Geometry geometry = sourceGeometry.convertGeometry(Gba.GEOMETRY_FACTORY_2D);
      return geometry.newValidGeometry();
    }
  }

  public SitePointProviderRecord newSitePoint(final AbstractSiteConverter siteConverter,
    final Point point) {
    final SitePointProviderRecord sitePoint = new SitePointProviderRecord(siteConverter,
      RECORD_DEFINITION);
    sitePoint.setGeometryValue(point);

    communities.setBoundaryIdAndName(sitePoint, point, COMMUNITY_NAME);
    regionalDistricts.setBoundaryIdAndName(sitePoint, point, REGIONAL_DISTRICT_NAME);

    return sitePoint;
  }

  protected Comparator<Record> newSitePointComparator() {
    return (record1, record2) -> {
      String name1 = record1.getString(STREET_NAME);
      NameDirection nameDirectionPrefix1 = NameDirection.NONE;
      try {
        final int spaceIndex = name1.indexOf(' ');
        if (spaceIndex > -1) {
          final String firstPart = name1.substring(0, spaceIndex);
          if (firstPart.length() < 2) {
            nameDirectionPrefix1 = NameDirection.valueOf(firstPart);
            name1 = name1.substring(spaceIndex + 1);
          }
        }
      } catch (final Throwable e) {
      }
      String name2 = record2.getString(STREET_NAME);
      NameDirection nameDirectionPrefix2 = NameDirection.NONE;
      try {
        final int spaceIndex = name2.indexOf(' ');
        if (spaceIndex > -1) {
          final String firstPart = name2.substring(0, spaceIndex);

          if (firstPart.length() < 2) {
            nameDirectionPrefix2 = NameDirection.valueOf(firstPart);
            name2 = name2.substring(spaceIndex + 1);
          }
        }
      } catch (final Throwable e) {
      }
      int compare = CompareUtil.compare(name1, name2, true);
      if (compare == 0) {
        compare = CompareUtil.compare(nameDirectionPrefix1, nameDirectionPrefix2, true);
        if (compare == 0) {
          compare = record1.compareValue(record2, CIVIC_NUMBER, true);
          if (compare == 0) {
            compare = record1.compareValue(record2, CIVIC_NUMBER_SUFFIX, true);
            if (compare == 0) {
              compare = record1.compareValue(record2, UNIT_DESCRIPTOR, true);
            }
          }
        }
      }
      return compare;
    };
  }

  @Override
  protected void postConvertRecord(final SitePointProviderRecord sitePoint) {
    super.postConvertRecord(sitePoint);
    final String range = sitePoint.getCivicNumberRange();
    if (range.length() > 0) {
      final int index = range.indexOf('~');
      final int civicNumber1 = Integer.parseInt(range.substring(0, index));
      final int civicNumber2 = Integer.parseInt(range.substring(index + 1));

      addWithCivicNumber(sitePoint, civicNumber1);
      addWithCivicNumber(sitePoint, civicNumber2);

    } else {
      this.recordsByLocalityName.addValue(this.localityName, sitePoint);
    }
  }

  @Override
  protected void postConvertRecords() {
    postConvertRecordsWriteSitePoints();
    addProviderBoundary(this.sourceGeometryByLocality);
  }

  protected void postConvertRecordsWriteLocality(final String localityName,
    final List<Record> localityRecords) {
    final PartnerOrganization partnerOrganization = this.partnerOrganizationFiles
      .getPartnerOrganization();
    try (
      AtomicPathUpdator localityPathUpdator = ImportSites.SITE_POINT_BY_LOCALITY
        .newLocalityPathUpdator(this.dialog, this.baseDirectory, localityName, partnerOrganization,
          this.fileSuffix);
      RecordWriter localityWriter = RecordWriter.newRecordWriter(RECORD_DEFINITION,
        localityPathUpdator.getPath())) {
      localityWriter.writeAll(localityRecords);
    }
  }

  protected void postConvertRecordsWriteSitePoints() {
    if (!this.recordsByLocalityName.isEmpty()) {
      final Comparator<Record> comparator = newSitePointComparator();

      final Path dataProviderPath = this.pathUpdator.getPath();
      try (
        RecordWriter dataProviderWriter = RecordWriter.newRecordWriter(RECORD_DEFINITION,
          dataProviderPath)) {

        for (final Entry<String, List<Record>> localityEntry : cancellable(
          this.recordsByLocalityName.entrySet())) {
          final String localityName = localityEntry.getKey();
          final List<Record> localityRecords = localityEntry.getValue();

          localityRecords.sort(comparator);

          for (final Record record : cancellable(localityRecords)) {
            dataProviderWriter.write(record);
          }

          postConvertRecordsWriteLocality(localityName, localityRecords);
        }
      }
    }
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

  public void setFeatureStatusCodeByFullAddress(final Record sitePoint, String fullAddress) {
    if (fullAddress != null) {
      final String localityName = Strings.upperCase(this.localityName);
      fullAddress = Strings.upperCase(fullAddress);
      final FeatureStatus featureStatusCode = Maps.getMap(
        ProviderSitePointConverter.featureStatusByLocalityAndFullAddress, localityName, fullAddress,
        FeatureStatus.ACTIVE);
      if (featureStatusCode.isIgnored()) {
        final String message = "Ignored FULL_ADDRESS in FULL_ADDRESS_FEATURE_STATUS_CODE.xlsx";
        throw IgnoreSiteException.warning(message);
      } else {
        sitePoint.setValue(FEATURE_STATUS_CODE, featureStatusCode.getCode());
      }
    }
  }

  public void setIdFieldName(final String idFieldName) {
    this.idFieldName = idFieldName;
  }

  public void setOpenData(final boolean openData) {
    if (openData) {
      this.openDataInd = "Y";
    } else {
      this.openDataInd = "N";
    }
  }

  public void setPathUpdator(final AtomicPathUpdator pathUpdator) {
    this.pathUpdator = pathUpdator;
    this.baseDirectory = pathUpdator.getTargetDirectory() //
      .getParent();
  }

  public void setProviderConfig(final ProviderSitePointConverter providerConfig) {
    this.providerConfig = providerConfig;
    final boolean openData = this.providerConfig.isOpenData();
    setOpenData(openData);
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
        addStructuredNameError(message, originalName, null, null);

        throw IgnoreSiteException.warning(message);
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
        addStructuredNameError(message, originalName, structuredName, structuredNameId);
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
