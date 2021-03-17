package ca.bc.gov.gbasites.load;

import java.awt.FlowLayout;
import java.awt.Rectangle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.swing.BorderFactory;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.core.model.CountNames;
import ca.bc.gov.gba.core.model.Gba;
import ca.bc.gov.gba.itn.GbaItnDatabase;
import ca.bc.gov.gba.itn.model.code.GbaItnCodeTables;
import ca.bc.gov.gba.itn.model.code.StructuredNames;
import ca.bc.gov.gba.process.AbstractTaskByLocality;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.common.DirectorySuffixAndExtension;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.load.convert.SiteConverterAddress;
import ca.bc.gov.gbasites.load.convert.SiteConverterCKRD;
import ca.bc.gov.gbasites.load.convert.SiteConverterParts;
import ca.bc.gov.gbasites.load.merge.LoadEmergencyManagementSites;
import ca.bc.gov.gbasites.load.merge.RecordMergeCounters;
import ca.bc.gov.gbasites.load.merge.SitePointMerger;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBC;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSiteConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSplitByProvider;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBC;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBcSiteConverter;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBcSplitByProvider;
import ca.bc.gov.gbasites.load.readsource.SourceReaderArcGis;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFile;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFileGdb;
import ca.bc.gov.gbasites.load.readsource.SourceReaderJoin;
import ca.bc.gov.gbasites.load.readsource.SourceReaderMapGuide;
import ca.bc.gov.gbasites.load.write.WriteFgdbAll;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.CollectionMap;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.gis.esri.gdb.file.FileGdbWriter;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactory;
import com.revolsys.io.map.MapObjectFactoryRegistry;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.code.SingleValueCodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.spring.resource.ClassPathResource;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.component.BasePanel;
import com.revolsys.swing.field.CheckBox;
import com.revolsys.swing.field.ComboBox;
import com.revolsys.swing.layout.GroupLayouts;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;
import com.revolsys.util.Property;

public class ImportSites extends AbstractTaskByLocality implements SitePoint, CountNames {

  public static final String PROVIDER = "Provider";

  public static final String PROVIDERS = "Providers";

  public static StatisticsDialog dialog;

  public static final DirectorySuffixAndExtension ERROR_BY_PROVIDER = new DirectorySuffixAndExtension(
    "ErrorByProvider", "_ERROR", ".tsv");

  public static final DirectorySuffixAndExtension IGNORE_BY_PROVIDER = new DirectorySuffixAndExtension(
    "IgnoreByProvider", "_IGNORE", ".tsv");

  public static final DirectorySuffixAndExtension WARNING_BY_PROVIDER = new DirectorySuffixAndExtension(
    "WarningByProvider", "_WARNING", ".tsv");

  public static final Path LOCALITY_DIRECTORY = GbaController.getDataDirectory("Sites/Locality");

  public static final String MATCHED = "Matched";

  public static final String MERGED_READ = "M Read";

  public static final String MERGED_WRITE = "M Write";

  public static final DirectorySuffixAndExtension NAME_ERROR_BY_PROVIDER = new DirectorySuffixAndExtension(
    "NameErrorByProvider", "_NAME_ERROR", ".xlsx");

  public static final String PROVIDER_MISSING = "P Missing";

  private static final long serialVersionUID = 1L;

  public static final DirectorySuffixAndExtension SITE_POINT = new DirectorySuffixAndExtension(
    "SitePoint", "_SITE_POINT", ".tsv");

  public static final DirectorySuffixAndExtension SITE_POINT_TO_DELETE = new DirectorySuffixAndExtension(
    "SitePointToDelete", "_SITE_POINT_TO_DELETE", ".tsv");

  public static final DirectorySuffixAndExtension SITE_POINT_BY_LOCALITY = new DirectorySuffixAndExtension(
    "SitePointByLocality", "_SITE_POINT", ".tsv");

  public static final DirectorySuffixAndExtension SITE_POINT_BY_PROVIDER = new DirectorySuffixAndExtension(
    "SitePointByProvider", "_SITE_POINT", ".tsv");

  public static final Path SITES_DIRECTORY = GbaController.getDataDirectory("Sites");

  public static final Path SITES_TEMP_DIRECTORY = SITES_DIRECTORY.resolve("Temp");

  public static final Map<String, String> siteTypeByBuildingType = new LinkedHashMap<>();

  public static final DirectorySuffixAndExtension SOURCE_BY_PROVIDER = new DirectorySuffixAndExtension(
    "SourceByProvider", "_SOURCE", ".tsv");

  public static final String PROVIDER_SPLIT = "P Split";

  public static final String ABC_SPLIT = AddressBC.COUNT_PREFIX + "Split";

  public static final String TO_DELETE = "To Delete";

  public static final String PROVIDER_READ = "P Read";

  public static void deleteTempFiles(final Path directory) {
    try {
      Files.list(directory).forEach(path -> {
        final String fileName = Paths.getFileName(path);
        if (fileName.startsWith("_")) {
          if (!Paths.deleteDirectories(path)) {
            Logs.error(GbaController.class, "Unable to remove temporary files from: " + path);
          }
        }
      });
    } catch (final IOException e) {
      Logs.error(GbaController.class, "Unable to remove temporary files from: " + directory, e);
    }
  }

  public static RecordDefinitionImpl getSitePointFgdbRecordDefinition() {
    RecordDefinitionImpl recordDefinition = getSitePointTsvRecordDefinition();
    recordDefinition = new RecordDefinitionImpl(recordDefinition);
    for (final FieldDefinition field : recordDefinition.getFields()) {
      field.setRequired(false);
    }
    recordDefinition.setIdFieldName(null);
    return recordDefinition;
  }

  public static RecordDefinitionImpl getSitePointTsvRecordDefinition() {
    if (AbstractSiteConverter.sitePointTsvRecordDefinition == null) {
      final Path recordDefinitionFile = ProviderSitePointConverter.SITE_CONFIG_DIRECTORY
        .resolve("Schema")
        .resolve("SITE_POINT.json");
      Paths.createParentDirectories(recordDefinitionFile);
      final RecordDefinitionImpl recordDefinition;
      if (Paths.exists(recordDefinitionFile)) {
        recordDefinition = MapObjectFactory.toObject(recordDefinitionFile);
        recordDefinition.setIdFieldName(SITE_ID);
      } else {
        final RecordDefinition sitePointRecordDefinition = GbaSiteDatabase.getRecordStore()
          .getRecordDefinition(SiteTables.SITE_POINT);
        recordDefinition = new RecordDefinitionImpl(PathName.newPathName("SITE_POINT"));
        recordDefinition.setDefaultValues(sitePointRecordDefinition.getDefaultValues());
        for (final FieldDefinition sitePointField : sitePointRecordDefinition.getFields()) {
          final String fieldName = sitePointField.getName();
          final FieldDefinition tsvField = new FieldDefinition(sitePointField);
          recordDefinition.addField(tsvField);
          if (fieldName.startsWith(STREET_NAME)) {
            final String newFieldName = fieldName.replace("_ID", "");
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING, 100, false);
            }
          } else if (fieldName.endsWith("_PARTNER_ORG_ID")) {
            final String newFieldName = fieldName.replace("_ID", "");
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING, 30, false);
            }
          } else if (fieldName.equals(LOCALITY_ID)) {
            final String newFieldName = LOCALITY_NAME;
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING, 50, false);
            }
          } else if (fieldName.equals(COMMUNITY_ID)) {
            final String newFieldName = COMMUNITY_NAME;
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING, 50, false);
            }
          } else if (fieldName.equals(REGIONAL_DISTRICT_ID)) {
            final String newFieldName = REGIONAL_DISTRICT_NAME;
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING, 50, false);
            }
          }
        }
        recordDefinition.setGeometryFactory(Gba.GEOMETRY_FACTORY_2D);
        recordDefinition.writeToFile(recordDefinitionFile);
      }
      AbstractSiteConverter.sitePointTsvRecordDefinition = recordDefinition;

    }
    return AbstractSiteConverter.sitePointTsvRecordDefinition;

  }

  public static void initializeService() {

    MapObjectFactoryRegistry.newFactory("gbaSiteConverterAddress",
      SiteConverterAddress::newFactory);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterCKRD", SiteConverterCKRD::newFactory);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterParts", SiteConverterParts::newFactory);

    MapObjectFactoryRegistry.newFactory("gbaSiteLoader", ProviderSitePointConverter::new);

    MapObjectFactoryRegistry.newFactory("sourceReaderArcGis", SourceReaderArcGis::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderFile", SourceReaderFile::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderFileGdb", SourceReaderFileGdb::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderJoin", SourceReaderJoin::newFactory);
    MapObjectFactoryRegistry.newFactory("sourceReaderMapGuide", SourceReaderMapGuide::newFactory);
  }

  public static void main(final String[] args) {
    initializeService();
    start(ImportSites.class);
  }

  public static AtomicPathUpdator newPathUpdator(final Cancellable cancellable,
    final Path directory, final String fileName) {
    return new AtomicPathUpdator(SITES_TEMP_DIRECTORY, cancellable, directory, fileName);
  }

  private final CheckBox addressBcConvertCheckbox = new CheckBox("addressBcConvert", true);

  private final CheckBox addressBcDownloadCheckbox = new CheckBox("addressBcDownload", true);

  private final CheckBox addressBcSplitCheckbox = new CheckBox("addressBcSplit", true);

  private final CheckBox geoBcConvertCheckbox = new CheckBox("geoBcConvert", true);

  private final CheckBox geoBcSplitCheckbox = new CheckBox("geoBcSplit", true);

  private final List<ProviderSitePointConverter> dataProvidersToProcess = Collections
    .synchronizedList(new LinkedList<>());

  private final CheckBox fgdbCheckbox = new CheckBox("fgdb", true);

  private final CheckBox mergeCheckbox = new CheckBox("merge", true);

  private final Map<String, Identifier> partnerOrganizationIdByShortName = new HashMap<>();

  private ComboBox<ProviderSitePointConverter> providerComboBox;

  private final CheckBox providerConvertCheckbox = new CheckBox("providerConvertCheckbox", true);

  private final CheckBox providerDownloadCheckbox = new CheckBox("providerDownload", true);

  public ImportSites() {
    super("Import Sites");
    dialog = this;
    setThreadCount(8);
    setLockName(null);
    setUseTotalCounts(false);
    setLogChanges(false);
    setShowAllLocalityCheckbox(false);

    ProviderSitePointConverter.init();
  }

  private void action1Download() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      converter.downloadData(this, true);
    };

    final boolean downloadData = this.providerDownloadCheckbox.isSelected();
    final ProcessNetwork processes = new ProcessNetwork();
    if (downloadData) {
      addConverterProcesses("Downloading", processes, providerAction);
    }
    processes.addProcess("Download and Split " + AddressBC.NAME, () -> {
      final boolean download = this.addressBcDownloadCheckbox.isSelected();
      final boolean split = this.addressBcSplitCheckbox.isSelected();
      AddressBcSplitByProvider.split(this, download, split);
    });
    if (this.geoBcSplitCheckbox.isSelected()) {
      processes.addProcess(GeoBC.COUNT_PREFIX + "Split", () -> {
        new GeoBcSplitByProvider(dialog).run();
      });
    }
    setSelectedTab(PROVIDER);
    processes.startAndWait();
  }

  private void action2Convert() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      converter.convertData(this);
    };

    final boolean convert = this.providerConvertCheckbox.isSelected();
    try {
      ProviderSitePointConverter.preProcess();
      final ProcessNetwork processes = new ProcessNetwork();
      if (convert) {
        addConverterProcesses("Converting", processes, providerAction);
      }
      if (this.addressBcConvertCheckbox.isSelected()) {
        processes.addProcess("Convert " + AddressBC.NAME, () -> {
          AddressBcSiteConverter.convertAll(this);
        });
      }

      if (this.geoBcConvertCheckbox.isSelected()) {
        processes.addProcess("Convert " + GeoBC.NAME, () -> {
          GeoBcSiteConverter.convertAll(this);
        });
      }

      setSelectedTab(PROVIDER);
      processes.startAndWait();
    } finally {
      ProviderSitePointConverter.postProcess(this.dataProvidersToProcess);
    }
  }

  private void action4WriteFgdb() {
    if (!isCancelled() && this.fgdbCheckbox.isSelected()) {
      labelCounts("FGDB", "Locality Name", //
        "Provider", //
        GeoBC.NAME, //
        AddressBC.NAME, //
        "EM", //
        "All", //
        "Merged");
      setSelectedTab("FGDB");
      Paths.createDirectories(SITES_DIRECTORY.resolve("FGDB"));
      final CollectionMap<String, Record, List<Record>> emergencyManagementSitesByLocality = new LoadEmergencyManagementSites()
        .loadEmergencyManagementSites(this);
      new ProcessNetwork() //
        .addProcess("FGDB-Merged", () -> writeFgdbMerged(emergencyManagementSitesByLocality)) //
        .addProcess("FGDB-ALL", new WriteFgdbAll(this, emergencyManagementSitesByLocality)) //
        .startAndWait();
    }
  }

  private void addConverterProcesses(final String label, final ProcessNetwork processes,
    final Consumer<ProviderSitePointConverter> action) {
    if (!this.dataProvidersToProcess.isEmpty()) {
      final LinkedList<ProviderSitePointConverter> converters = (LinkedList<ProviderSitePointConverter>)Lists
        .toList(LinkedList::new, this.dataProvidersToProcess);
      for (int i = 0; i < getThreadCount(); i++) {
        processes.addProcess("Provider " + label + " " + (i + 1), () -> {
          while (!isCancelled()) {
            final ProviderSitePointConverter converter;
            synchronized (converters) {
              if (converters.isEmpty()) {
                return;
              } else {
                converter = converters.removeFirst();
              }
            }

            try {
              action.accept(converter);
            } catch (final Throwable e) {
              Logs.error(ProviderSitePointConverter.class,
                "Error " + label + "\n" + converter.getPartnerOrganizationFileName(), e);
            }
          }
        });
      }
    }
  }

  private void addProviderCounts(final LabelCountMapTableModel providerCounts,
    final String countPrefix) {
    providerCounts.addColumns( //
      countPrefix + "Source", //
      countPrefix + "Converted", //
      countPrefix + "Ignored", //
      countPrefix + "Error", //
      countPrefix + "Warning" //
    );
  }

  @Override
  protected void adjustSize() {
    final Rectangle bounds = SwingUtil.getScreenBounds();

    setLocation(bounds.x + 20, bounds.y + 20);

    final int width = Math.min(1550, bounds.width - 40);
    final int height = bounds.height - 40;
    setSize(width, height);
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    try {
      Paths.deleteDirectories(SITES_TEMP_DIRECTORY);
    } catch (final Exception e) {
    }
    Paths.createDirectories(SITES_TEMP_DIRECTORY);
    for (final String directory : Arrays.asList(AddressBC.NAME, PROVIDER, GeoBC.NAME)) {
      final Path baseDirectory = SITES_DIRECTORY.resolve(directory);
      NAME_ERROR_BY_PROVIDER.createDirectory(baseDirectory);
      ERROR_BY_PROVIDER.createDirectory(baseDirectory);
      IGNORE_BY_PROVIDER.createDirectory(baseDirectory);
      WARNING_BY_PROVIDER.createDirectory(baseDirectory);
      SITE_POINT_BY_LOCALITY.createDirectory(baseDirectory);
      SITE_POINT.createDirectory(baseDirectory);
      SITE_POINT_BY_PROVIDER.createDirectory(baseDirectory);
      SOURCE_BY_PROVIDER.createDirectory(baseDirectory);
    }
    loadCodes();

    action1Download();
    action2Convert();

    initPartnerOrganizationShortNames();
    if (!isCancelled()) {
      if (this.mergeCheckbox.isSelected()) {
        setSelectedTab(ProviderSitePointConverter.LOCALITY);
        SITE_POINT.createDirectory(SITES_DIRECTORY);
        SITE_POINT_TO_DELETE.createDirectory(SITES_DIRECTORY);
        super.batchUpdate(transaction);
      }
    }
    action4WriteFgdb();

    if (!isCancelled()) {
      final Path countsDirectory = SITES_DIRECTORY.resolve("Counts");
      Paths.createDirectories(countsDirectory);
      writeCounts(countsDirectory, "xlsx");
    }
    return !isCancelled();
  }

  public Identifier getPartnerOrganizationByShortName(final String shortName) {
    return this.partnerOrganizationIdByShortName.get(shortName);
  }

  private void initPartnerOrganizationShortNames() {
    // TODO disabled while we are using files
    // final CodeTable partnerOrganizations =
    // GbaController.getPartnerOrganizations();
    // for (final Identifier partnerOrganizationId :
    // partnerOrganizations.getIdentifiers()) {
    // final String partnerOrganizationName =
    // partnerOrganizations.getValue(partnerOrganizationId);
    // String shortName = null;
    // if (partnerOrganizationName.startsWith("Locality - ")) {
    // shortName = partnerOrganizationName.substring(11);
    // } else if (partnerOrganizationName.startsWith("Regional District - ")) {
    // shortName = partnerOrganizationName.substring(20);
    // } else if (partnerOrganizationName.startsWith("Provider - ")) {
    // shortName = partnerOrganizationName.substring(11);
    // } else {
    // shortName = partnerOrganizationName;
    // }
    // if (shortName != null) {
    // shortName = shortName.replace(' ', '_').toUpperCase();
    // this.partnerOrganizationIdByShortName.put(shortName,
    // partnerOrganizationId);
    // }
    // }
  }

  private void loadCodes() {

    final StructuredNames structuredNames = GbaItnCodeTables.getStructuredNames();
    structuredNames.setLoadAll(true);
    structuredNames.setLoadMissingCodes(false);
    structuredNames.refresh();

    AbstractSiteConverter.init();

    loadCodes(SiteTables.FEATURE_STATUS_CODE);
    loadCodes(SiteTables.SITE_LOCATION_CODE);
    loadSiteTypeCodes();
  }

  private void loadCodes(final PathName pathName) {
    loadCodes(pathName, null);
  }

  private void loadCodes(final PathName pathName, final Consumer<Record> action) {
    final String typeName = pathName.getName();
    final RecordStore recordStore = GbaItnDatabase.getRecordStore();
    final ClassPathResource resource = new ClassPathResource(
      "/ca/bc/gov/gba/schema/codes/" + typeName + ".tsv");
    try (
      Transaction transaction = recordStore.newTransaction();
      RecordReader reader = RecordReader.newRecordReader(resource)) {
      if (recordStore.getRecordDefinition(pathName) == null) {
        final SingleValueCodeTable codeTable = new SingleValueCodeTable(typeName);
        for (final Record record : reader) {
          final Identifier code = record.getIdentifier(typeName);
          final String description = record.getString("DESCRIPTION");
          codeTable.addValue(code, description);
        }
        recordStore.addCodeTable(typeName, codeTable);
      } else {
        for (final Record record : reader) {
          final String code = record.getString(typeName);
          final String description = record.getString("DESCRIPTION");
          if (recordStore.getRecord(pathName, code) == null) {
            final Record codeRecord = recordStore.newRecord(pathName);
            codeRecord.setIdentifier(Identifier.newIdentifier(code));
            codeRecord.setValue("DESCRIPTION", description);
            final Identifier integrationSessionId = getIntegrationSessionId();
            codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
            codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
            recordStore.insertRecord(codeRecord);
            if (action != null) {
              action.accept(record);
            }
          }
        }
      }
    }
  }

  private void loadSiteTypeCodes() {
    loadCodes(SiteTables.SITE_TYPE_CODE, record -> {
      final String plGroup = record.getString("PL_GROUP");
      if (Property.hasValue(plGroup)) {
        final String plType = record.getString("PL_TYPE");
        for (final String type : plType.split(",")) {
          final String code = record.getString(SITE_TYPE_CODE);
          siteTypeByBuildingType.put(plGroup + "-" + type, code);
        }
      }
    });
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new SitePointMerger(this);
  }

  @Override
  protected BasePanel newPanelOptions() {
    return new BasePanel(//
      newPanelOptionsAddressBc(), //
      newPanelOptionsGeoBC(), //
      newPanelOptionsProvider(), //
      newPanelOptionsLocalities(), //
      newPanelOptionsFgdb() //
    );
  }

  private BasePanel newPanelOptionsAddressBc() {
    final BasePanel downloadPanel = new BasePanel(new FlowLayout(FlowLayout.LEFT, 2, 2), //
      SwingUtil.newLabel("Download"), //
      this.addressBcDownloadCheckbox, //
      SwingUtil.newLabel("Split"), //
      this.addressBcSplitCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.addressBcConvertCheckbox //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder(AddressBC.NAME));
    return downloadPanel;
  }

  private BasePanel newPanelOptionsFgdb() {

    final BasePanel fieldPanel = new BasePanel(//
      SwingUtil.newLabel("Write FDGB"), //
      this.fgdbCheckbox //
    );
    GroupLayouts.makeColumns(fieldPanel, 2, true);
    fieldPanel.setBorder(BorderFactory.createTitledBorder("FGDB"));
    return fieldPanel;
  }

  private BasePanel newPanelOptionsGeoBC() {
    final BasePanel downloadPanel = new BasePanel(new FlowLayout(FlowLayout.LEFT, 2, 2), //
      SwingUtil.newLabel("Split"), //
      this.geoBcSplitCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.geoBcConvertCheckbox //
    );
    downloadPanel
      .setBorder(BorderFactory.createTitledBorder(GeoBC.NAME + " (Supplemental Address)"));
    return downloadPanel;
  }

  private BasePanel newPanelOptionsLocalities() {
    final BasePanel optionsPanel = super.newPanelOptions();
    optionsPanel.addComponents(//
      SwingUtil.newLabel("Merge"), //
      this.mergeCheckbox);
    GroupLayouts.makeColumns(optionsPanel, 2, true);
    optionsPanel.setBorder(BorderFactory.createTitledBorder("Merge Sites"));
    return optionsPanel;
  }

  private BasePanel newPanelOptionsProvider() {
    final List<ProviderSitePointConverter> dataProviders = new ArrayList<>();
    dataProviders.add(null);
    dataProviders.addAll(ProviderSitePointConverter.getLoaders());
    this.providerComboBox = ComboBox.newComboBox("dataProvider", dataProviders);

    final BasePanel fieldPanel = new BasePanel(//
      SwingUtil.newLabel("Data Provider"), //
      this.providerComboBox //
    );
    GroupLayouts.makeColumns(fieldPanel, 2, true);

    final BasePanel checkboxPanel = new BasePanel(new FlowLayout(FlowLayout.LEFT, 2, 2), //
      SwingUtil.newLabel("Download"), //
      this.providerDownloadCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.providerConvertCheckbox);

    final BasePanel panel = new BasePanel(fieldPanel, checkboxPanel);
    panel.setBorder(BorderFactory.createTitledBorder("Provider: Download and Convert"));
    return panel;
  }

  @Override
  protected void setOptions(final BasePanel optionsPanel) {
    super.setOptions(optionsPanel);
    final ProviderSitePointConverter dataProviderToProcess = this.providerComboBox
      .getSelectedItem();

    if (dataProviderToProcess == null) {
      this.dataProvidersToProcess.addAll(ProviderSitePointConverter.getLoaders());
    } else {
      this.dataProvidersToProcess.add(dataProviderToProcess);
    }
    final boolean hasProviders = !this.dataProvidersToProcess.isEmpty();

    final LabelCountMapTableModel providerCounts = labelCounts(PROVIDER, //
      "Data Provider" //
    );
    addProviderCounts(providerCounts, "P ");
    addProviderCounts(providerCounts, GeoBC.COUNT_PREFIX);
    addProviderCounts(providerCounts, AddressBC.COUNT_PREFIX);

    final String PROVIDER_READ = "P Read";
    final String GEOBC_READ = GeoBC.COUNT_PREFIX + " Read";
    final String ABC_READ = AddressBC.COUNT_PREFIX + "Read";

    final String PROVIDER_USED = "P Used";
    final String GEOBC_USED = GeoBC.COUNT_PREFIX + " Used";
    final String ABC_USED = AddressBC.COUNT_PREFIX + "Used";
    final String TOTAL_USED = "Total Used";

    final LabelCountMapTableModel localityCounts = labelCounts(ProviderSitePointConverter.LOCALITY, //
      ProviderSitePointConverter.LOCALITY, //

      PROVIDER_READ, //
      PROVIDER_USED, //
      GEOBC_READ, //
      GEOBC_USED, //
      ABC_READ, //
      ABC_USED, //
      TOTAL_USED, //

      MERGED_READ, //

      MATCHED, //
      INSERTED, //
      UPDATED, //
      DELETED, //
      TO_DELETE, //
      MERGED_WRITE //
    );

    final LabelCountMapTableModel providersCounters = RecordMergeCounters.addProviderCounts(this,
      PROVIDERS);
    final LabelCountMapTableModel geobcCounters = RecordMergeCounters.addProviderCounts(this,
      GeoBC.NAME);
    final LabelCountMapTableModel addressBcCounters = RecordMergeCounters.addProviderCounts(this,
      AddressBC.NAME);

    localityCounts.addTotalColumn(PROVIDER_READ) //
      .addCounters(providersCounters.getLabelCounters(READ)) //
    ;
    localityCounts.addTotalColumn(PROVIDER_USED) //
      .addCounters(providersCounters.getLabelCounters(RecordMergeCounters.USED)) //
    ;
    localityCounts.addTotalColumn(GEOBC_READ) //
      .addCounters(geobcCounters.getLabelCounters(READ)) //
    ;
    localityCounts.addTotalColumn(GEOBC_USED) //
      .addCounters(geobcCounters.getLabelCounters(RecordMergeCounters.USED)) //
    ;
    localityCounts.addTotalColumn(ABC_READ) //
      .addCounters(addressBcCounters.getLabelCounters(READ)) //
    ;
    localityCounts.addTotalColumn(ABC_USED) //
      .addCounters(addressBcCounters.getLabelCounters(RecordMergeCounters.USED)) //
    ;
    localityCounts.addTotalColumn(TOTAL_USED, PROVIDER_USED, GEOBC_USED, ABC_USED);

    for (final String localityName : getLocalityNamesToProcess()) {
      localityCounts.addRowLabel(localityName);
    }
    labelCounts(LoadEmergencyManagementSites.EM_SITES, "Type Name", GBA_READ, PROVIDER_READ,
      LoadEmergencyManagementSites.IGNORE_XCOVER, INSERTED, UPDATED);
    if (hasProviders) {
      labelCounts(CountNames.ERROR, //
        "Message", //
        CountNames.ERROR//
      );

      labelCounts(ProviderSitePointConverter.NAME_ERRORS, //
        "Message", //
        CountNames.ERROR//
      );

      labelCounts(ProviderSitePointConverter.WARNING, "Message", //
        ProviderSitePointConverter.WARNING);
    }
  }

  private void writeFgdbMerged(
    final CollectionMap<String, Record, List<Record>> emergencyManagementSitesByLocality) {
    try (
      AtomicPathUpdator pathUpdator = ImportSites.newPathUpdator(this,
        SITES_DIRECTORY.resolve("FGDB"), "SITE_POINT.gdb");
      FileGdbRecordStore recordStore = FileGdbRecordStoreFactory
        .newRecordStore(pathUpdator.getPath());) {
      final RecordDefinitionImpl recordDefinition = getSitePointFgdbRecordDefinition();
      try (
        FileGdbWriter writer = recordStore.newRecordWriter(recordDefinition)) {

        final Collection<String> localityNames = GbaItnCodeTables.getLocalities()
          .getBoundaryNames();
        for (final String localityName : cancellable(localityNames)) {
          final String localityFileName = Gba.toFileName(localityName);
          final Counter writeCounter = getCounter("FGDB", localityName, "Merged");

          final List<Record> records = emergencyManagementSitesByLocality.getOrEmpty(localityName);
          if (!records.isEmpty()) {
            for (final Record record : records) {
              writer.writeNewRecord(record);
              writeCounter.add();
            }
          }

          final Path localityFile = SITE_POINT.getLocalityFilePath(SITES_DIRECTORY,
            localityFileName);
          try (
            RecordReader reader = RecordReader.newRecordReader(localityFile)) {
            for (final Record record : cancellable(reader)) {
              writer.writeNewRecord(record);
              writeCounter.add();
            }
          }
        }
      }
    }
  }

}
