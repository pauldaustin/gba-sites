package ca.bc.gov.gbasites.load.common;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;

import org.jeometry.common.data.type.DataType;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizationProxy;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.load.readsource.AbstractSourceReader;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.code.FeatureStatus;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactory;
import com.revolsys.properties.BaseObjectWithProperties;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.util.Property;
import com.revolsys.util.ServiceInitializer;

public class ProviderSitePointConverter extends BaseObjectWithProperties
  implements SitePoint, PartnerOrganizationProxy {

  public final static Map<String, Map<String, Geometry>> boundaryByProviderAndLocality = new TreeMap<>();

  public static boolean calculateBoundary;

  public static final Map<String, Map<String, FeatureStatus>> featureStatusByLocalityAndFullAddress = new LinkedHashMap<>();

  public static final String LOCALITY = "Locality";

  public static final String NAME_ERRORS = "Name Errors";

  public static final Path PROVIDER_DIRECTORY = ImportSites.SITES_DIRECTORY.resolve("Provider");

  public static final Path SITE_CONFIG_DIRECTORY = GbaController.getConfigPath("Sites");

  static final Map<String, ProviderSitePointConverter> siteLoaderByDataProvider = new TreeMap<>();

  public static final String WARNING = "Warning";

  public static Collection<ProviderSitePointConverter> getLoaders() {
    return siteLoaderByDataProvider.values();
  }

  public static RecordDefinition getSourceWriterRecordDefinition(
    final RecordDefinition recordDefinition, final GeometryFactory forceGeometryFactory) {
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

  public static void init() {
    ServiceInitializer.initializeServices();

    final Path siteProviderConfigPath = SITE_CONFIG_DIRECTORY.resolve("Provider/OpenData");
    if (Paths.exists(siteProviderConfigPath)) {
      try {
        Files.list(siteProviderConfigPath).forEach(path -> {
          try {
            final String fileNameExtension = Paths.getFileNameExtension(path);
            if ("json".equals(fileNameExtension)) {
              final ProviderSitePointConverter loader = MapObjectFactory.toObject(path);
              if (loader.isEnabled()) {
                final String dataProvider = loader.getDataProvider();
                siteLoaderByDataProvider.put(dataProvider, loader);
              }
            }
          } catch (final Throwable e) {
            Logs.error(ProviderSitePointConverter.class, "Unable to load config:" + path, e);
          }
        });
      } catch (final Throwable e) {
        Logs.error(ProviderSitePointConverter.class,
          "Unable to load config:" + siteProviderConfigPath, e);
      }
    }
  }

  public static boolean isCalculateBoundary() {
    return calculateBoundary;
  }

  public static void postProcess(final List<ProviderSitePointConverter> dataProvidersToProcess) {
    {
      final Path path = PROVIDER_DIRECTORY.resolve("PROVIDER_BOUNDARIES.tsv");
      Gba.GEOMETRY_FACTORY_2D.writePrjFile(path);
      try (
        TsvWriter writer = Tsv.plainWriter(path)) {
        writer.write("PROVIDER_NAME", "LOCALITY_NAME", "GEOMETRY");
        for (final Entry<String, Map<String, Geometry>> providerEntry : boundaryByProviderAndLocality
          .entrySet()) {
          final String partnerOrganizationName = providerEntry.getKey();
          final Map<String, Geometry> boundaryByLocality = providerEntry.getValue();
          for (final Entry<String, Geometry> localityEntry : boundaryByLocality.entrySet()) {
            final String name = localityEntry.getKey();
            final Geometry geometry = localityEntry.getValue();
            writer.write(partnerOrganizationName, name, geometry);
          }
        }
      } catch (final Throwable e) {
      }
    }
  }

  public static void preProcess() {
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

  private Function<MapEx, AbstractSiteConverter> converter;

  private String dataProvider;

  private boolean enabled = true;

  private boolean openData = false;

  private PartnerOrganization partnerOrganization;

  private Function<MapEx, AbstractSourceReader> sourceReader;

  public ProviderSitePointConverter(final Map<String, ? extends Object> properties) {
    setProperties(properties);
  }

  public void convertData(final StatisticsDialog dialog, final boolean convert) {
    if (this.converter == null) {
      Logs.error(this, "No site point converter for: " + this.partnerOrganization);
    } else {
      final PartnerOrganizationFiles partnerOrganizationFiles = newPartnerOrganizationFiles(dialog);
      final MapEx properties = new LinkedHashMapEx() //
        .add("baseDirectory", PROVIDER_DIRECTORY) //
        .add("partnerOrganizationFiles", partnerOrganizationFiles) //
        .add("countPrefix", "P ") //
        .add("dialog", dialog) //
        .add("openData", this.openData) //
      ;
      final AbstractSiteConverter converter = this.converter.apply(properties);
      converter.convertSourceRecords(convert);
    }
  }

  public void downloadData(final StatisticsDialog dialog, final boolean downloadData) {
    if (this.sourceReader == null) {
      Logs.error(this, "No source reader for: " + this.partnerOrganization);
    } else {
      final PartnerOrganizationFiles partnerOrganizationFiles = newPartnerOrganizationFiles(dialog);
      final MapEx properties = new LinkedHashMapEx() //
        .add("baseDirectory", PROVIDER_DIRECTORY) //
        .add("partnerOrganizationFiles", partnerOrganizationFiles) //
        .add("countPrefix", "P ") //
        .add("dialog", dialog) //
      ;
      final AbstractSourceReader readerProcess = this.sourceReader.apply(properties);
      readerProcess.downloadData(downloadData);
    }
  }

  public String getDataProvider() {
    return this.dataProvider;
  }

  @Override
  public PartnerOrganization getPartnerOrganization() {
    return this.partnerOrganization;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public boolean isOpenData() {
    return this.openData;
  }

  private PartnerOrganizationFiles newPartnerOrganizationFiles(final StatisticsDialog dialog) {
    return new PartnerOrganizationFiles(dialog, this.partnerOrganization, PROVIDER_DIRECTORY, "");
  }

  public void setConverter(final Function<MapEx, AbstractSiteConverter> converter) {
    this.converter = converter;
  }

  public void setDataProvider(final String dataProvider) {
    this.dataProvider = dataProvider;
    this.partnerOrganization = GbaSiteDatabase.newPartnerOrganization(dataProvider);
  }

  public void setEnabled(final boolean enabled) {
    this.enabled = enabled;
  }

  public void setOpenData(final boolean openData) {
    this.openData = openData;
  }

  public void setSourceReader(final Function<MapEx, AbstractSourceReader> readerFactory) {
    this.sourceReader = readerFactory;
  }

  @Override
  public String toString() {
    return this.dataProvider;
  }
}
