package ca.bc.gov.gbasites.load.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.PartnerOrganizations;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.common.converter.AbstractSiteConverter;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.set.Sets;
import com.revolsys.geometry.index.PointRecordMap;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.Point;
import com.revolsys.geometry.model.Polygonal;
import com.revolsys.geometry.operation.union.UnaryUnionOp;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.io.Writer;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactory;
import com.revolsys.io.map.MapSerializer;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordLog;
import com.revolsys.record.Records;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.ListRecordReader;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.esri.rest.map.FeatureLayer;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Debug;
import com.revolsys.util.Property;
import com.revolsys.util.ServiceInitializer;
import com.revolsys.util.Strings;
import com.revolsys.webservice.WebServiceFeatureLayer;

public class ProviderSitePointConverter extends BaseObjectWithProperties
  implements SitePoint, Runnable {
  private static final List<String> FIELD_NAMES_CUSTODIAN_SITE_ID = Collections
    .singletonList(CUSTODIAN_SITE_ID);

  private static final Comparator<Record> COMPARATOR_CUSTODIAN_SITE_ID = Record
    .newComparatorIdentifier(CUSTODIAN_SITE_ID);

  static final Map<String, String> ADDRESS_BC_PROVIDER_ALIAS = Maps.<String, String> buildHash() //
    .add("GREATER VANCOUVER REGIONAL DISTRICT", "GVRD") //
    .add("MOUNT WADDINGTON REGIONAL DISTRICT", "MWRD") //
    .add("TOWN OF VIEW ROYAL", "View Royal") //
    .add("POWELL RIVER CITY", "Powell River") //
    .add("SQUAMISH LILLOOET RD RURAL", "SLRD") //
    .add("TOWN OF LADYSMITH", "Ladysmith") //
    .add("STSAILES", "Chehalis") //
    .add("VILLAGE OF FRUITVALE", "Fruitvale") //
    .add("VILLAGE OF MIDWAY", "Midway") //
    .add("VILLAGE OF WARFIELD", "Warfield") //
    .getMap();

  private final static Map<Identifier, Map<String, Geometry>> boundaryByProviderAndLocality = new TreeMap<>();

  private static boolean calculateBoundary;

  public static final String DATA_PROVIDER = "Data Provider";

  static ThreadLocal<Boolean> dataProviderOpenDataThread = new ThreadLocal<>();

  private static final Map<String, Map<String, FeatureStatus>> featureStatusByLocalityAndFullAddress = new LinkedHashMap<>();

  private final static Set<String> icisIgnoreIssuingAgencies = Sets.newHash();

  public static final String IGNORE_ADDRESS_BC = "Ignore AddressBC";

  public static final String READ_ADDRESS_BC = "Read AddressBC";

  public static final String IGNORED = "Ignored";

  static final String INTEGRATED_CADASTRAL_INFORMATION_SOCIETY = "Integrated Cadastral Information Society";

  private static final BoundaryCache localities = GbaController.getLocalities();

  public static final String LOCALITY = "Locality";

  static ThreadLocal<String> localityNameForThread = new ThreadLocal<>();

  public static final String NAME_ERRORS = "Name Errors";

  private static final Map<String, Identifier> PARTNER_ORGANIZATION_ID_BY_DATA_PROVIDER = new HashMap<>();

  private static final Map<Identifier, String> PARTNER_ORGANIZATION_SHORT_NAMES = new HashMap<>();

  private static final Map<String, Identifier> partnerOrganizationIdByIssuingAgency = new HashMap<>();

  static ThreadLocal<Identifier> partnerOrganizationIdThread = new ThreadLocal<>();

  public static final String PROVIDER_CONVERT = "Provider Convert";

  private static Map<Identifier, RecordLog> recordLogByPartnerOrganizationId = new HashMap<>();

  public static final String SECONDARY = "Secondary";

  public static final Path SITE_CONFIG_DIRECTORY = GbaController.getGbaPath().resolve("etc/Sites");

  static final Map<String, ProviderSitePointConverter> siteLoaderByDataProvider = new TreeMap<>();

  static RecordDefinitionImpl sitePointTsvRecordDefinition;

  static ThreadLocal<Record> sourceRecordForThread = new ThreadLocal<>();

  public static final String WARNING = "Warning";

  private static RecordWriter providerConfigWriter;

  public static void addError(final String message) {
    final Identifier partnerOrganizationId = getPartnerOrganizationIdThread();
    final String partnerOrganizationName = getPartnerOrganizationNameThread();
    final Record record = sourceRecordForThread.get();
    ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
      BatchUpdateDialog.ERROR);
    ImportSites.dialog.addLabelCount(BatchUpdateDialog.ERROR, message, BatchUpdateDialog.ERROR);

    RecordLog errorLog = recordLogByPartnerOrganizationId.get(partnerOrganizationId);
    if (errorLog == null) {
      final Path dataProviderErrorFile = getDataProviderFile(partnerOrganizationId, "_ERROR.tsv",
        "_");
      errorLog = new RecordLog(true);
      final RecordDefinition logRecordDefinition = errorLog.getLogRecordDefinition(record);
      final RecordWriter errorWriter = RecordWriter.newRecordWriter(logRecordDefinition,
        dataProviderErrorFile);
      errorLog.setWriter(errorWriter);
      recordLogByPartnerOrganizationId.put(partnerOrganizationId, errorLog);
    }
    Geometry geometry = record.getGeometry();
    if (geometry != null) {
      geometry = geometry.getPointWithin();
    }
    final String localityName = getLocalityName();
    errorLog.error(localityName, message, record, geometry);
  }

  static void addProviderBoundary(final Identifier partnerOrganizationId,
    final Map<String, List<Geometry>> sourceGeometryByLocality) {
    if (calculateBoundary) {
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
          Maps.addToMap(Maps.factoryTree(), boundaryByProviderAndLocality, partnerOrganizationId,
            localityName, boundary);
        }
      }
    }

  }

  public static void addWarningCount(final String message) {
    final String partnerOrganizationName = getPartnerOrganizationNameThread();
    ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
      ProviderSitePointConverter.WARNING);
    ImportSites.dialog.addLabelCount(ProviderSitePointConverter.WARNING, message,
      ProviderSitePointConverter.WARNING);
  }

  public static Path getDataProviderDirectory(final Identifier partnerOrganizationId,
    final String prefix) {
    final Path dataDirectory = ImportSites.SITES_DIRECTORY.resolve("Provider");
    String partnerOrganizationShortName = getPartnerOrganizationShortName(partnerOrganizationId);

    if (prefix != null) {
      partnerOrganizationShortName = prefix + partnerOrganizationShortName;
    }
    final Path directory = dataDirectory.resolve(partnerOrganizationShortName);
    if (prefix != null) {
      if (Paths.exists(directory)) {
        // Paths.
      }
    }
    Paths.createDirectories(directory);
    return directory;
  }

  public static Path getDataProviderFile(final Identifier partnerOrganizationId,
    final String suffix, final String prefix) {
    final Path dataProviderDirectory = getDataProviderDirectory(partnerOrganizationId, prefix);
    final String baseName = getFileName(partnerOrganizationId);
    final Path dataProviderErrorFile = dataProviderDirectory.resolve(baseName + suffix);
    return dataProviderErrorFile;
  }

  static String getFileName(final Identifier partnerOrganizationId) {
    final String shortName = getPartnerOrganizationShortName(partnerOrganizationId);
    return BatchUpdateDialog.toFileName(shortName);
  }

  protected static Collection<ProviderSitePointConverter> getLoaders() {
    final List<ProviderSitePointConverter> providers = new ArrayList<>();
    providers.add(null);
    for (final ProviderSitePointConverter provider : siteLoaderByDataProvider.values()) {
      if (provider.getDataProvider().equals(INTEGRATED_CADASTRAL_INFORMATION_SOCIETY)) {
        providers.set(0, provider);
      } else {
        providers.add(provider);
      }
    }
    return providers;
  }

  public static String getLocalityName() {
    return localityNameForThread.get();
  }

  public static Identifier getPartnerOrganizationId(String dataProvider) {
    Identifier partnerOrganizationId = PARTNER_ORGANIZATION_ID_BY_DATA_PROVIDER
      .get(dataProvider.toUpperCase());
    if (partnerOrganizationId == null) {
      String shortName;
      final Identifier localityId = localities.getIdentifier(dataProvider);
      if (localityId == null) {
        final Identifier regionalDistrictId = GbaController.getRegionalDistricts()
          .getIdentifier(dataProvider);
        if (regionalDistrictId == null) {
          shortName = dataProvider;
          dataProvider = "Provider - " + dataProvider;
        } else {
          shortName = regionalDistrictId.getString(0);
          dataProvider = "Regional District - " + regionalDistrictId;
        }
      } else {
        shortName = localities.getValue(localityId);
        dataProvider = "Locality - " + shortName;
      }
      partnerOrganizationId = PartnerOrganizations.getId(dataProvider);
      PARTNER_ORGANIZATION_SHORT_NAMES.put(partnerOrganizationId, shortName);
      PARTNER_ORGANIZATION_ID_BY_DATA_PROVIDER.put(dataProvider.toUpperCase(),
        partnerOrganizationId);
    }
    partnerOrganizationIdThread.set(partnerOrganizationId);
    return partnerOrganizationId;
  }

  public static Identifier getPartnerOrganizationIdThread() {
    return partnerOrganizationIdThread.get();
  }

  static String getPartnerOrganizationName(final Identifier partnerOrganizationId) {
    return GbaController.getPartnerOrganizations().getValue(partnerOrganizationId);
  }

  public static String getPartnerOrganizationNameThread() {
    final Identifier partnerOrganizationId = getPartnerOrganizationIdThread();
    return getPartnerOrganizationName(partnerOrganizationId);
  }

  public static String getPartnerOrganizationShortName() {
    final Identifier partnerOrganizationId = getPartnerOrganizationIdThread();
    return getPartnerOrganizationShortName(partnerOrganizationId);
  }

  public static String getPartnerOrganizationShortName(final Identifier partnerOrganizationId) {
    String shortName = PARTNER_ORGANIZATION_SHORT_NAMES.get(partnerOrganizationId);
    if (shortName == null) {
      final String name = GbaController.getPartnerOrganizations().getValue(partnerOrganizationId);
      if (name != null) {
        final int index = name.indexOf(" - ");
        if (index != -1) {
          shortName = name.substring(index + 3);
          PARTNER_ORGANIZATION_SHORT_NAMES.put(partnerOrganizationId, shortName);
          return shortName;
        }
      }
      return null;

    }
    return shortName;
  }

  public static RecordDefinitionImpl getSitePointTsvRecordDefinition() {
    if (sitePointTsvRecordDefinition == null) {
      final RecordDefinition sitePointRecordDefinition = GbaSiteDatabase.getRecordStore()
        .getRecordDefinition(SiteTables.SITE_POINT);

      final RecordDefinitionImpl recordDefinition = new RecordDefinitionImpl(
        PathName.newPathName("SITE_POINT"));
      recordDefinition.setDefaultValues(sitePointRecordDefinition.getDefaultValues());
      for (final FieldDefinition sitePointField : sitePointRecordDefinition.getFields()) {
        final String fieldName = sitePointField.getName();
        final FieldDefinition tsvField = new FieldDefinition(sitePointField);
        recordDefinition.addField(tsvField);
        if (fieldName.startsWith("STREET_NAME") || fieldName.endsWith("_PARTNER_ORG_ID")) {
          final String newFieldName = fieldName.replace("_ID", "");
          if (!sitePointRecordDefinition.hasField(newFieldName)) {
            recordDefinition.addField(newFieldName, DataTypes.STRING);
          }
        }
      }
      recordDefinition.setGeometryFactory(Gba.GEOMETRY_FACTORY_2D);
      sitePointTsvRecordDefinition = recordDefinition;
    }
    return sitePointTsvRecordDefinition;
  }

  static RecordDefinition getSourceWriterRecordDefinition(final RecordDefinition recordDefinition,
    final GeometryFactory forceGeometryFactory) {
    final RecordDefinitionImpl sourceWriterRecordDefinition = new RecordDefinitionImpl(
      recordDefinition.getPathName());
    final String geometryFieldName = recordDefinition.getGeometryFieldName();
    for (final FieldDefinition fieldDefinition : recordDefinition.getFields()) {
      String fieldName = fieldDefinition.getName();
      final DataType fieldType = fieldDefinition.getDataType();
      if (geometryFieldName.equals(fieldName)) {
        fieldName = "GEOMETRY";
      }
      sourceWriterRecordDefinition.addField(fieldName, fieldType);
    }
    GeometryFactory geometryFactory = recordDefinition.getGeometryFactory();
    if (geometryFactory == null || geometryFactory.getHorizontalCoordinateSystemId() == 0) {
      if (forceGeometryFactory != null) {
        geometryFactory = forceGeometryFactory;
      }
    }
    sourceWriterRecordDefinition.setGeometryFactory(geometryFactory);
    return sourceWriterRecordDefinition;
  }

  static Geometry getValidSourceGeometry(final Geometry sourceGeometry) {
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

  protected static void init() {
    ServiceInitializer.initializeServices();
    siteLoaderByDataProvider.put(INTEGRATED_CADASTRAL_INFORMATION_SOCIETY,
      new ProviderSitePointConverter(INTEGRATED_CADASTRAL_INFORMATION_SOCIETY,
        ProviderSitePointConverter::loadSitesIntegratedCadastralInformationSociety));

    final Path siteProviderConfigPath = SITE_CONFIG_DIRECTORY.resolve("Provider/OpenData");
    if (Paths.exists(siteProviderConfigPath)) {
      final RecordDefinition providerConfigRecordDefinition = new RecordDefinitionBuilder("/CONFIG") //
        .addField(DATA_PROVIDER, DataTypes.STRING, 30) //
        .addField("Open Data", DataTypes.BOOLEAN) //
        .addField("Type", DataTypes.STRING, 15) //
        .addField("Source URL", DataTypes.STRING, 80) //
        .addField("Source Path", DataTypes.STRING, 80) //
        .addField("Geometry URL", DataTypes.STRING, 50) //
        .addField("Geometry Path", DataTypes.STRING, 50) //
        .getRecordDefinition();

      providerConfigWriter = RecordWriter.newRecordWriter(providerConfigRecordDefinition,
        ImportSites.SITES_DIRECTORY.resolve("PROVIDER_CONFIG.xlsx"));
      try (
        RecordWriter providerConfigWriter = ProviderSitePointConverter.providerConfigWriter) {
        Files.list(siteProviderConfigPath).forEach(path -> {
          try {
            final String fileNameExtension = Paths.getFileNameExtension(path);
            if ("json".equals(fileNameExtension)) {
              final ProviderSitePointConverter loader = MapObjectFactory.toObject(path);
              if (loader.isEnabled()) {
                final String dataProvider = loader.getDataProvider();
                icisIgnoreIssuingAgencies.add(dataProvider.toUpperCase());

                final Identifier partnerOrganizationId = getPartnerOrganizationId(dataProvider);
                siteLoaderByDataProvider.put(dataProvider, loader);

                final List<String> issuingAgencies = loader.getIssuingAgencies();
                for (String issuingAgency : issuingAgencies) {
                  issuingAgency = issuingAgency.toUpperCase();
                  icisIgnoreIssuingAgencies.add(issuingAgency);
                  partnerOrganizationIdByIssuingAgency.put(issuingAgency, partnerOrganizationId);
                  ADDRESS_BC_PROVIDER_ALIAS.put(issuingAgency, dataProvider);
                }
              }
            }
          } catch (final Throwable e) {
            Logs.error(ProviderSitePointConverter.class, "Unable to load config:" + path, e);
          }
        });
      } catch (final Throwable e) {
        Logs.error(ProviderSitePointConverter.class,
          "Unable to load config:" + siteProviderConfigPath, e);
      } finally {
        ProviderSitePointConverter.providerConfigWriter = null;
      }
    }
  }

  static SitePointProviderRecord loadSite(final Identifier partnerOrgId,
    final String partnerOrganizationName, final AbstractSiteConverter sitePointConverter,
    final Map<Identifier, Map<String, List<Record>>> sitesByLocalityIdAndFullAddress,
    final Map<String, List<Geometry>> geometriesByLocality, final Record sourceRecord,
    final GeometryFactory forceGeometryFactory) {
    SitePointProviderRecord sitePoint = null;
    sourceRecordForThread.set(sourceRecord);
    ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
      BatchUpdateDialog.READ);
    Geometry sourceGeometry = sourceRecord.getGeometry();
    if (Property.isEmpty(sourceGeometry)) {
      ProviderSitePointConverter.addWarningCount("Ignore Record does not contain a point geometry");
      ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
        ProviderSitePointConverter.IGNORED);
    } else {
      if (forceGeometryFactory != null) {
        sourceGeometry = sourceGeometry.newUsingGeometryFactory(forceGeometryFactory);
      }
      final Geometry convertedSourceGeometry = ProviderSitePointConverter
        .getValidSourceGeometry(sourceGeometry);
      boolean addToBoundary = true;
      String localityName = null;

      try {
        final Point point = convertedSourceGeometry.getPointWithin();

        final Identifier localityId = localities.getBoundaryId(point);
        localityName = localities.getValue(localityId);
        localityNameForThread.set(localityName);
        if (Property.isEmpty(point) || !point.isValid()) {
          addToBoundary = false;
          throw new IgnoreSiteException("Ignore Record does not contain a point geometry");
        } else {
          sitePoint = sitePointConverter.convert(sourceRecord, point);
          if (sitePoint == null) {
            ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
              ProviderSitePointConverter.IGNORED);
          } else {
            if (dataProviderOpenDataThread.get() == Boolean.TRUE) {
              sitePoint.setValue(OPEN_DATA_IND, "Y");
            } else {
              sitePoint.setValue(OPEN_DATA_IND, "N");
            }
            sitePoint.setValue(LOCALITY_ID, localityId);

            sitePoint.updateFullAddress();
            sitePoint.setValue(CREATE_PARTNER_ORG_ID, partnerOrgId);
            sitePoint.setValue(MODIFY_PARTNER_ORG_ID, partnerOrgId);
            sitePoint.setValue(CUSTODIAN_PARTNER_ORG_ID, partnerOrgId);

            if (!sitePoint.hasValue(CUSTODIAN_SITE_ID)) {
              Debug.noOp();
            }
            final String siteKey = sitePoint.getFullAddress();
            Maps.addToList(Maps.factoryTree(), sitesByLocalityIdAndFullAddress, localityId, siteKey,
              sitePoint);
          }
        }
      } catch (final IgnoreSiteException e) {
        final String countName = e.getCountName();
        if (ProviderSitePointConverter.IGNORE_ADDRESS_BC.equals(countName)) {
          ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
            ProviderSitePointConverter.IGNORE_ADDRESS_BC);
          addToBoundary = false;
        } else {
          if (e.isError()) {
            addError(countName);
          } else {
            addWarningCount(countName);
          }

          ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
            ProviderSitePointConverter.IGNORED);
        }
      } catch (final NullPointerException e) {
        Logs.error(ImportSites.class, "Null pointer", e);
        addError("Null Pointer");
        ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
          ProviderSitePointConverter.IGNORED);
      } catch (final Throwable e) {
        addError(e.getMessage());
        ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
          ProviderSitePointConverter.IGNORED);
      }
      if (addToBoundary && localityName != null) {
        ImportSites.dialog.addLabelCount(LOCALITY, localityName, PROVIDER_CONVERT);
        if (calculateBoundary) {
          Maps.addToList(geometriesByLocality, localityName, convertedSourceGeometry);
        }
      }
    }
    return sitePoint;
  }

  private static int loadSites(final Identifier partnerOrganizationId,
    final Iterable<Record> sourceSites, final GeometryFactory forceGeometryFactory,
    final AbstractSiteConverter sitePointConverter, final RecordWriter sourceRecordWriter,
    final int exepctedRecordCount) {
    int recordCount = 0;
    if (!ImportSites.dialog.isCancelled()) {
      final String partnerOrganizationName = GbaController.getPartnerOrganizations()
        .getValue(partnerOrganizationId);
      final Map<String, List<Geometry>> sourceGeometryByLocality = new TreeMap<>();
      {
        final Map<Identifier, Map<String, List<Record>>> sitesByLocalityIdAndFullAddress = new HashMap<>();
        for (final Record sourceRecord : ImportSites.dialog.cancellable(sourceSites)) {
          recordCount++;
          loadSite(partnerOrganizationId, partnerOrganizationName, sitePointConverter,
            sitesByLocalityIdAndFullAddress, sourceGeometryByLocality, sourceRecord,
            forceGeometryFactory);
          writeSourceRecord(sourceRecordWriter, sourceRecord);
        }
        if (exepctedRecordCount == -1 || exepctedRecordCount == recordCount) {
          writeSitePoints(partnerOrganizationId, sitesByLocalityIdAndFullAddress);
        }
      }
      addProviderBoundary(partnerOrganizationId, sourceGeometryByLocality);
    }
    return recordCount;
  }

  static void loadSitesIntegratedCadastralInformationSociety() {
    // TODO load from pre-generated files
  }

  private static void loadSitesReaderFactory(final ProviderSitePointConverter loader,
    final Supplier<RecordReader> sourceReaderFactory,
    final RecordDefinition providerRecordDefinition, final int expectedRecordCount) {
    final Identifier partnerOrganizationId = loader.getPartnerOrganizationId();
    final GeometryFactory forceGeometryFactory = loader.getGeometryFactory();
    final Path newDirectory = getDataProviderDirectory(partnerOrganizationId, "_");
    try {
      int recordCount;
      final Path sourceTempFile = getDataProviderFile(partnerOrganizationId, "_PROVIDER.tsv", "_");
      try (
        RecordReader sourceReader = loader.newSourceRecordReader(ImportSites.isDownloadData(),
          sourceReaderFactory, providerRecordDefinition)) {
        final RecordDefinition sourceRecordDefinition = sourceReader.getRecordDefinition();
        final RecordDefinition sourceWriterRecordDefinition = ProviderSitePointConverter
          .getSourceWriterRecordDefinition(sourceRecordDefinition, forceGeometryFactory);
        try (
          RecordWriter sourceRecordWriter = RecordWriter
            .newRecordWriter(sourceWriterRecordDefinition, sourceTempFile)) {
          final AbstractSiteConverter sitePointConverter = loader.getConverter();

          recordCount = loadSites(partnerOrganizationId, sourceReader, null, sitePointConverter,
            sourceRecordWriter, expectedRecordCount);
        }
      }
      if (expectedRecordCount != -1) {
        if (expectedRecordCount != recordCount) {
          Logs.error(ProviderSitePointConverter.class, "Expecting site record count="
            + expectedRecordCount + " not " + recordCount + " for " + loader.getDataProvider());
          return;
        }
      }
      if (!ImportSites.dialog.isCancelled()) {
        replaceOldDirectory(partnerOrganizationId);
      }
    } finally {
      Paths.deleteDirectories(newDirectory);
    }
  }

  static ProviderSitePointConverter newProviderSitePointConverterArcGis(
    final Map<String, ? extends Object> properties) {
    final ProviderSitePointConverter loader = new ProviderSitePointConverter(properties);
    final String dataProvider = loader.getDataProvider();

    final String serverUrl = (String)properties.get("serverUrl");
    final String path = (String)properties.get("path");
    providerConfigWriter.write(dataProvider, loader.isOpenData(), "ArcGIS REST", serverUrl, path);

    final Runnable runnable = () -> {
      final boolean loadByObjectId = Maps.getBool(properties, "loadByObjectId");
      RecordDefinition layerRecordDefinition = null;
      Supplier<RecordReader> readerFactory = null;
      int expectedRecordCount = -1;
      try {
        final FeatureLayer layer = FeatureLayer.getRecordLayerDescription(serverUrl, path);
        if (layer == null) {
          Logs.error(ProviderSitePointConverter.class,
            dataProvider + ": using cached files. Cannot find layer: " + path + " on " + serverUrl);
        } else {
          expectedRecordCount = layer.getRecordCount((Query)null);
          layerRecordDefinition = layer.getRecordDefinition();
          readerFactory = () -> {
            if (loadByObjectId || !layer.isSupportsPagination()
              || layer.getCurrentVersion() < 10.3) {
              return layer.newRecordReader((Query)null, true);
            } else {
              return layer.newRecordReader(ArrayRecord.FACTORY, (Query)null);
            }
          };
        }
      } catch (final Throwable e) {
        Logs.error(ProviderSitePointConverter.class,
          dataProvider + ": using cached files. Cannot connect to server: " + serverUrl, e);
      }
      loadSitesReaderFactory(loader, readerFactory, layerRecordDefinition, expectedRecordCount);
    };
    loader.setRunnable(runnable);
    return loader;
  }

  @SuppressWarnings("unchecked")
  static ProviderSitePointConverter newProviderSitePointConverterFile(
    final Map<String, ? extends Object> properties) {
    final ProviderSitePointConverter loader = new ProviderSitePointConverter(properties);
    final String dataProvider = loader.getDataProvider();

    Supplier<RecordReader> sourceReaderFactory;
    String fileName = (String)properties.get("fileName");
    if (Property.hasValue(fileName)) {
      final Object source = ImportSites.SITES_DIRECTORY.resolve("Input")
        .resolve(dataProvider)
        .resolve(fileName);
      providerConfigWriter.write(dataProvider, loader.isOpenData(), "File",
        "Input/" + dataProvider + "/" + fileName);
      sourceReaderFactory = () -> {
        final RecordReader reader = RecordReader.newRecordReader(source);
        final Map<String, Object> readerProperties = (Map<String, Object>)properties
          .get("readerProperties");
        reader.setProperties(readerProperties);
        return reader;
      };
    } else {
      sourceReaderFactory = (Supplier<RecordReader>)properties.get("fileReaderFactory");
      if (sourceReaderFactory == null) {
        throw new IllegalArgumentException(
          "Config must have fileName, fileUrl, of fileReaderFactory:" + properties);
      } else if (sourceReaderFactory instanceof MapSerializer) {
        final MapSerializer serializer = (MapSerializer)sourceReaderFactory;
        final MapEx config = serializer.toMap();
        fileName = config.getString("fileName");
        final String fileUrl = config.getString("fileUrl");
        final String baseName = config.getString("baseName");
        final String baseFileExtension = config.getString("baseFileExtension");
        final String filePath;
        if (Property.hasValue(baseName)) {
          filePath = Strings.toString(".", baseName, baseFileExtension);
        } else {
          filePath = null;
        }
        if (Property.hasValue(fileName)) {
          providerConfigWriter.write(dataProvider, loader.isOpenData(), "File", fileName, filePath);
        } else if (Property.hasValue(fileUrl)) {
          providerConfigWriter.write(dataProvider, loader.isOpenData(), "File", fileUrl, filePath);
        }
      }
    }

    final Runnable runnable = () -> {
      loadSitesReaderFactory(loader, sourceReaderFactory, null, -1);
    };
    loader.setRunnable(runnable);
    return loader;
  }

  static ProviderSitePointConverter newProviderSitePointConverterFileGdb(
    final Map<String, ? extends Object> properties) {
    final ProviderSitePointConverter loader = new ProviderSitePointConverter(properties);
    final String dataProvider = loader.getDataProvider();
    final String gdbFileName = (String)properties.get("gdbFileName");
    final String typePath = (String)properties.get("typePath");
    providerConfigWriter.write(dataProvider, loader.isOpenData(), "FGDB",
      "Input/" + dataProvider + "/" + gdbFileName, typePath);
    final Runnable runnable = () -> {
      final Path sourceFile = ImportSites.SITES_DIRECTORY.resolve("Input")
        .resolve(dataProvider)
        .resolve(gdbFileName);

      try (
        FileGdbRecordStore recordStore = FileGdbRecordStoreFactory.newRecordStore(sourceFile)) {
        recordStore.setCreateMissingRecordStore(false);
        recordStore.setCreateMissingTables(false);
        recordStore.initialize();
        final Supplier<RecordReader> sourceReaderFactory = () -> {
          return recordStore.getRecords(PathName.newPathName(typePath));
        };
        loadSitesReaderFactory(loader, sourceReaderFactory, null, -1);
      }
    };
    loader.setRunnable(runnable);
    return loader;
  }

  @SuppressWarnings("unchecked")
  static ProviderSitePointConverter newProviderSitePointConverterJoin(
    final Map<String, ? extends Object> properties) {
    final ProviderSitePointConverter loader = new ProviderSitePointConverter(properties);
    final String dataProvider = loader.getDataProvider();
    final Supplier<RecordReader> fieldReaderFactory = (Supplier<RecordReader>)properties
      .get("fieldReaderFactory");
    String sourceUrl;
    String sourcePath;
    if (fieldReaderFactory instanceof MapSerializer) {
      final MapEx config = ((MapSerializer)fieldReaderFactory).toMap();
      final String fileUrl = config.getString("fileUrl");
      if (Property.hasValue(fileUrl)) {
        sourceUrl = fileUrl;
      } else {
        sourceUrl = config.getString("serverUrl");
      }
      sourcePath = config.getString("layerPath");
    } else {
      sourceUrl = null;
      sourcePath = null;
    }
    final Supplier<RecordReader> geometryReaderFactory = (Supplier<RecordReader>)properties
      .get("geometryReaderFactory");
    String geometryUrl;
    String geometryPath;
    if (fieldReaderFactory instanceof MapSerializer) {
      final MapEx config = ((MapSerializer)geometryReaderFactory).toMap();
      final String fileUrl = config.getString("fileUrl");
      if (Property.hasValue(fileUrl)) {
        geometryUrl = fileUrl;
      } else {
        geometryUrl = config.getString("serverUrl");
      }
      geometryPath = config.getString("layerPath");
    } else {
      geometryUrl = null;
      geometryPath = null;
    }
    providerConfigWriter.write(dataProvider, loader.isOpenData(), "Join", sourceUrl, sourcePath,
      geometryUrl, geometryPath);

    final Runnable runnable = () -> {
      final Supplier<RecordReader> readerFactory = () -> {
        final String geometryRecordFieldName = (String)properties.get("geometryRecordFieldName");

        final String fieldRecordFieldName = (String)properties.get("fieldRecordFieldName");

        GeometryFactory geometryFactory;
        final Map<String, Geometry> geometryById = new HashMap<>();
        try (
          RecordReader geometryReader = geometryReaderFactory.get()) {
          geometryFactory = geometryReader.getGeometryFactory();
          for (final Record geometryRecord : geometryReader) {
            final String id = geometryRecord.getString(geometryRecordFieldName);
            final Geometry geometry = geometryRecord.getGeometry();
            final Geometry geometry2d = geometry.newGeometry(2);
            geometryById.put(id, geometry2d);
          }
        }
        final List<Record> sourceSites = new ArrayList<>();
        final RecordDefinitionImpl recordDefinition;
        try (
          RecordReader fieldReader = fieldReaderFactory.get()) {
          recordDefinition = (RecordDefinitionImpl)fieldReader.getRecordDefinition();
          recordDefinition.addField("geometry", GeometryDataTypes.GEOMETRY);
          recordDefinition.setGeometryFactory(geometryFactory);

          for (final Record fieldRecord : fieldReader) {
            final String id = fieldRecord.getString(fieldRecordFieldName);
            final Geometry geometry = geometryById.get(id);
            if (geometry == null) {
              final String partnerOrganizationName = getPartnerOrganizationNameThread();
              ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
                ProviderSitePointConverter.IGNORED);
              ImportSites.dialog.addLabelCount(ProviderSitePointConverter.WARNING,
                "Ignore No matching geometry record", ProviderSitePointConverter.WARNING);
            } else {
              fieldRecord.setGeometryValue(geometry);
              sourceSites.add(fieldRecord);
            }
          }
        }
        return new ListRecordReader(recordDefinition, sourceSites);
      };
      loadSitesReaderFactory(loader, readerFactory, null, -1);
    };
    loader.setRunnable(runnable);
    return loader;
  }

  static ProviderSitePointConverter newProviderSitePointConverterMapGuide(
    final Map<String, ? extends Object> properties) {
    final ProviderSitePointConverter loader = new ProviderSitePointConverter(properties);
    final String dataProvider = loader.getDataProvider();

    final String serverUrl = (String)properties.get("serverUrl");
    final String path = (String)properties.get("path");
    providerConfigWriter.write(dataProvider, loader.isOpenData(), "MapGuide", serverUrl, path);

    final Runnable runnable = () -> {
      final GeometryFactory forceGeometryFactory = loader.getGeometryFactory();

      final WebServiceFeatureLayer layer = com.revolsys.record.io.format.mapguide.FeatureLayer
        .getFeatureLayer(serverUrl, path);
      if (layer == null) {
        Logs.error(ImportSites.class, "Cannot find layer: " + path + " on " + serverUrl);
      } else {
        final RecordDefinition layerRecordDefinition = layer.getRecordDefinition();
        if (forceGeometryFactory != null) {
          layerRecordDefinition.setGeometryFactory(forceGeometryFactory);
        }

        final Supplier<RecordReader> readerFactory = () -> {
          return layer.newRecordReader(ArrayRecord.FACTORY, (Query)null);
        };
        loadSitesReaderFactory(loader, readerFactory, layerRecordDefinition, -1);
      }
    };
    loader.setRunnable(runnable);
    return loader;
  }

  protected static void postProcess() {
    if (ImportSites.isProcessAllProviders() && !ImportSites.dialog.isCancelled()) {
      final Path providerCountsPath = ImportSites.SITES_DIRECTORY.resolve("PROVIDER_COUNTS.xlsx");
      final LabelCountMapTableModel providerCounts = ImportSites.dialog
        .getLabelCountTableModel(DATA_PROVIDER);
      providerCounts.writeCounts(providerCountsPath);
    }
    {
      final Path path = ImportSites.SITES_DIRECTORY.resolve("PROVIDER_BOUNDARIES.tsv");
      Gba.GEOMETRY_FACTORY_2D.writePrjFile(path);
      try (
        TsvWriter writer = Tsv.plainWriter(path)) {
        writer.write("PROVIDER_NAME", "LOCALITY_NAME", "GEOMETRY");
        for (final Entry<Identifier, Map<String, Geometry>> providerEntry : boundaryByProviderAndLocality
          .entrySet()) {
          final Identifier partnerOrganizationId = providerEntry.getKey();
          final Map<String, Geometry> boundaryByLocality = providerEntry.getValue();
          for (final Entry<String, Geometry> localityEntry : boundaryByLocality.entrySet()) {
            final String name = localityEntry.getKey();
            final Geometry geometry = localityEntry.getValue();
            final String partnerOrganizationName = ProviderSitePointConverter
              .getPartnerOrganizationName(partnerOrganizationId);
            writer.write(partnerOrganizationName, name, geometry);
          }
        }
      } catch (final Throwable e) {
      } finally {
        for (final RecordLog recordLog : recordLogByPartnerOrganizationId.values()) {
          try {
            recordLog.close();
          } catch (final Throwable e) {
            Logs.error(ImportSites.class, "Unable to close record log:" + recordLog, e);
          }
        }
        for (final RecordWriter writer : AbstractSiteConverter.nameDifferentWriterByPartnerOrganizationId
          .values()) {
          try {
            writer.close();
          } catch (final Throwable e) {
            Logs.error(ImportSites.class, "Unable to close writer:" + writer, e);
          }
        }
      }
    }
  }

  public static void preProcess() {
    final Path providersDirectory = ImportSites.SITES_DIRECTORY.resolve("Provider");
    try {
      Files.list(providersDirectory).forEach((final Path path) -> {
        final String fileName = Paths.getFileName(path);
        if (fileName.startsWith("_")) {
          if (!Paths.deleteDirectories(path)) {
            Logs.error(ProviderSitePointConverter.class,
              "Unable to remove temporary files from: " + path);
          }
        }
      });
    } catch (final IOException e) {
      Logs.error(ProviderSitePointConverter.class,
        "Unable to remove temporary files from: " + providersDirectory, e);
    }

    final Path fullAddressStatusPath = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
      .resolve("FULL_ADDRESS_FEATURE_STATUS_CODE.xlsx");
    if (Paths.exists(fullAddressStatusPath)) {
      try (
        RecordReader reader = RecordReader.newRecordReader(fullAddressStatusPath)) {
        for (final Record record : reader) {
          final String localityName = record.getUpperString("LOCALITY_NAME");
          final String fullAddress = record.getUpperString(FULL_ADDRESS);
          final FeatureStatus featureStatus = FeatureStatus.getFeatureStatus(record);
          if (Property.hasValuesAll(localityName, fullAddress, featureStatus)) {
            Maps.addToMap(featureStatusByLocalityAndFullAddress, localityName, fullAddress,
              featureStatus);
          }
        }
      } catch (final Throwable e) {
      }
    }
  }

  static void replaceOldDirectory(final Identifier partnerOrganizationId) {
    final Path newDirectory = getDataProviderDirectory(partnerOrganizationId, "_");
    final Path originalDirectory = getDataProviderDirectory(partnerOrganizationId, null);
    final Path originalTempDirectory = getDataProviderDirectory(partnerOrganizationId, "__");
    try {
      if (Paths.exists(originalDirectory)) {
        Files.move(originalDirectory, originalTempDirectory, StandardCopyOption.ATOMIC_MOVE);
      }
      try {
        Files.move(newDirectory, originalDirectory, StandardCopyOption.ATOMIC_MOVE);
      } catch (final IOException e) {
        Logs.error(ImportSites.class, "Error moving new directory: " + originalDirectory);
      }
    } catch (final IOException e) {
      Logs.error(ImportSites.class, "Error moving old directory: " + originalDirectory);
    } finally {
      if (Paths.exists(originalTempDirectory)) {
        Paths.deleteDirectories(originalTempDirectory);
      }
    }
  }

  public static void setFeatureStatusCodeByFullAddress(final Record sitePoint, String fullAddress) {
    if (fullAddress != null) {
      final String localityName = Strings.upperCase(getLocalityName());
      fullAddress = Strings.upperCase(fullAddress);
      final FeatureStatus featureStatusCode = Maps.getMap(featureStatusByLocalityAndFullAddress,
        localityName, fullAddress, FeatureStatus.ACTIVE);
      if (featureStatusCode.isIgnored()) {
        final String message = "Ignored FULL_ADDRESS in FULL_ADDRESS_FEATURE_STATUS_CODE.xlsx";
        throw new IgnoreSiteException(message);
      } else {
        sitePoint.setValue(FEATURE_STATUS_CODE, featureStatusCode.getCode());
      }
    }
  }

  static void writeSitePoints(final Identifier partnerOrganizationId,
    final Map<Identifier, Map<String, List<Record>>> sitesByLocalityIdAndFullAddress) {
    final String partnerOrganizationName = GbaController.getPartnerOrganizations()
      .getValue(partnerOrganizationId);
    final String dataProviderFilePart = ProviderSitePointConverter
      .getFileName(partnerOrganizationId);
    if (!sitesByLocalityIdAndFullAddress.isEmpty()) {
      final Path dataProviderFile = ProviderSitePointConverter
        .getDataProviderFile(partnerOrganizationId, "_SITE_POINT.tsv", "_");
      final RecordDefinitionImpl recordDefinition = ProviderSitePointConverter
        .getSitePointTsvRecordDefinition();
      try (
        Writer<Record> dataProviderWriter = RecordWriter.newRecordWriter(recordDefinition,
          dataProviderFile)) {
        for (final Entry<Identifier, Map<String, List<Record>>> localityEntry : ImportSites.dialog
          .cancellable(sitesByLocalityIdAndFullAddress.entrySet())) {
          final Identifier localityId = localityEntry.getKey();
          String localityName = localities.getValue(localityId);
          if (localityName == null) {
            localityName = "Unknown";
          }
          final Map<String, List<Record>> sitesByFullAddress = localityEntry.getValue();

          final String localityFilePart = BatchUpdateDialog.toFileName(localityName);
          final String localityFileName;
          if (localityFilePart.equals(dataProviderFilePart)) {
            localityFileName = localityFilePart + "_SITE_POINT.tsv";
          } else {
            localityFileName = localityFilePart + "_SITE_POINT_" + dataProviderFilePart + ".tsv";
          }
          final Path localityFile = ImportSites.LOCALITY_DIRECTORY.resolve(localityFileName);
          try (
            Writer<Record> localityWriter = RecordWriter.newRecordWriter(recordDefinition,
              localityFile)) {
            for (final List<Record> sitesWithSameFullAddress : ImportSites.dialog
              .cancellable(sitesByFullAddress.values())) {
              sitesWithSameFullAddress.sort(COMPARATOR_CUSTODIAN_SITE_ID);
              // Remove duplicate records with the same point location
              if (sitesWithSameFullAddress.size() > 1) {
                final PointRecordMap sitesByPoint = new PointRecordMap(sitesWithSameFullAddress);
                for (final Point point : sitesByPoint.getKeys()) {
                  final List<Record> sitesWithSamePoint = sitesByPoint.getRecords(point);
                  sitesWithSamePoint.sort(COMPARATOR_CUSTODIAN_SITE_ID);
                  if (sitesWithSamePoint.size() > 1) {
                    for (int i = 0; i < sitesWithSamePoint.size(); i++) {
                      final Record site1 = sitesWithSamePoint.get(i);
                      for (int j = i + 1; j < sitesWithSamePoint.size();) {
                        final Record site2 = sitesWithSamePoint.get(j);
                        final List<String> differentFieldNames = site1
                          .getDifferentFieldNames(site2);
                        boolean remove = false;
                        if (differentFieldNames.isEmpty()) {
                          remove = true;
                        } else {
                          if (differentFieldNames.equals(FIELD_NAMES_CUSTODIAN_SITE_ID)) {
                            // Keep the lowest ID, values already sorted.
                            remove = true;
                          }
                        }
                        if (remove) {
                          sitesWithSamePoint.remove(j);
                          sitesWithSameFullAddress.remove(site2);
                          addError("Ignore duplicate FULL_ADDRESS and point");
                        } else {
                          j++;
                        }
                      }
                    }
                  }
                }
              }

              Record closetRecord = null;
              double closestDistance = Double.MAX_VALUE;
              if (sitesWithSameFullAddress.size() > 1) {
                final Geometry allGeometries = Records.getGeometry(sitesWithSameFullAddress);
                final Point centroid = allGeometries.getCentroid();
                for (final Record record : ImportSites.dialog
                  .cancellable(sitesWithSameFullAddress)) {
                  final Point point = record.getGeometry();
                  final double distance = point.distancePoint(centroid);
                  if (distance < closestDistance) {
                    closetRecord = record;
                    closestDistance = distance;
                  }
                }
              }

              for (final Record site : ImportSites.dialog.cancellable(sitesWithSameFullAddress)) {
                if (!(site == closetRecord || closetRecord == null)) {
                  site.setValue(USE_IN_ADDRESS_RANGE_IND, "N");
                  ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
                    ProviderSitePointConverter.SECONDARY);
                }
                dataProviderWriter.write(site);
                localityWriter.write(site);
                ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
                  BatchUpdateDialog.WRITE);
              }
            }
          }
        }
      }
    }
  }

  static void writeSourceRecord(final RecordWriter writer, final Record record) {
    if (writer != null) {
      final Record writeRecord = writer.newRecord(record);
      final Geometry geometry = record.getGeometry();
      if (geometry != null) {
        final Geometry geometry2d = geometry.newGeometry(2);
        writeRecord.setGeometryValue(geometry2d);
      }
      writer.write(writeRecord);
    }
  }

  private AbstractSiteConverter converter;

  private String dataProvider;

  private boolean enabled = true;

  private GeometryFactory geometryFactory;

  private List<String> issuingAgencies = Collections.emptyList();

  private boolean openData = false;

  private Runnable runnable;

  public ProviderSitePointConverter(final Map<String, ? extends Object> properties) {
    setProperties(properties);
    final Object converterValue = properties.get("converter");
    if (converterValue instanceof AbstractSiteConverter) {
      this.converter = (AbstractSiteConverter)converterValue;
    } else {
      throw new IllegalArgumentException("Unknown converter:" + converterValue);
    }
  }

  public ProviderSitePointConverter(final Map<String, ? extends Object> properties,
    final Runnable runnable) {
    this(properties);
    this.runnable = runnable;
  }

  public ProviderSitePointConverter(final String dataProvider, final Runnable runnable) {
    setDataProvider(dataProvider);
    this.runnable = runnable;
  }

  public AbstractSiteConverter getConverter() {
    return this.converter;
  }

  public Integer getCoordinateSystemId() {
    if (this.geometryFactory == null) {
      return null;
    } else {
      return this.geometryFactory.getHorizontalCoordinateSystemId();
    }
  }

  public String getDataProvider() {
    return this.dataProvider;
  }

  public GeometryFactory getGeometryFactory() {
    return this.geometryFactory;
  }

  public List<String> getIssuingAgencies() {
    return this.issuingAgencies;
  }

  public Identifier getPartnerOrganizationId() {
    return ProviderSitePointConverter.getPartnerOrganizationId(this.dataProvider);
  }

  public String getPartnerOrganizationName() {
    if (this.dataProvider == ProviderSitePointConverter.INTEGRATED_CADASTRAL_INFORMATION_SOCIETY) {
      return this.dataProvider;
    } else {
      final Identifier partnerOrganizationId = getPartnerOrganizationId(this.dataProvider);
      return getPartnerOrganizationName(partnerOrganizationId);
    }
  }

  public Runnable getRunnable() {
    return this.runnable;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public boolean isOpenData() {
    return this.openData;
  }

  public RecordReader newSourceRecordReader(final boolean downloadData,
    final Supplier<RecordReader> sourceReaderFactory,
    final RecordDefinition providerRecordDefinition) {
    final Identifier partnerOrganizationId = getPartnerOrganizationId();
    final Path sourceFile = ProviderSitePointConverter.getDataProviderFile(partnerOrganizationId,
      "_PROVIDER.tsv", null);
    final boolean download = downloadData || !Files.exists(sourceFile);
    RecordReader reader;
    if (download && sourceReaderFactory != null) {
      reader = sourceReaderFactory.get();
    } else {
      reader = RecordReader.newRecordReader(sourceFile);
      if (reader != null && providerRecordDefinition != null) {
        final RecordDefinition sitesRecordDefinition = reader.getRecordDefinition();
        for (final FieldDefinition field : sitesRecordDefinition.getFields()) {
          final String name = field.getName();
          final CodeTable codeTable = providerRecordDefinition.getCodeTableByFieldName(name);
          if (codeTable != null) {
            field.setCodeTable(codeTable);
          }
        }
      }
    }
    return reader;
  }

  @Override
  public void run() {
    try {
      if (this.dataProvider == ProviderSitePointConverter.INTEGRATED_CADASTRAL_INFORMATION_SOCIETY) {
        dataProviderOpenDataThread.set(false);
      } else {
        final Identifier partnerOrganizationId = getPartnerOrganizationId();
        final String partnerOrganizationName = ProviderSitePointConverter
          .getPartnerOrganizationName(partnerOrganizationId);
        ImportSites.dialog.addLabelCount(DATA_PROVIDER, partnerOrganizationName,
          BatchUpdateDialog.READ, 0);
        dataProviderOpenDataThread.set(isOpenData());
      }
      this.runnable.run();

    } catch (final Throwable e) {
      Logs.error(this, "Error loading: " + this.dataProvider, e);
    }
  }

  public void setConverter(final AbstractSiteConverter converter) {
    this.converter = converter;
  }

  public void setCoordinateSystemId(final Integer coordinateSystemId) {
    if (coordinateSystemId == null) {
      this.geometryFactory = null;
    } else {
      this.geometryFactory = GeometryFactory.floating2d(coordinateSystemId);
    }
  }

  public void setDataProvider(final String dataProvider) {
    this.dataProvider = dataProvider;
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public void setGeometryFactory(final GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
  }

  public void setIssuingAgencies(final List<String> issuingAgencies) {
    this.issuingAgencies = issuingAgencies;
  }

  public void setOpenData(final boolean openData) {
    this.openData = openData;
  }

  public void setRunnable(final Runnable runnable) {
    this.runnable = runnable;
  }

  @Override
  public String toString() {
    return this.dataProvider;
  }
}
