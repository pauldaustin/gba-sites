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
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.process.AbstractTaskByLocality;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.load.common.DirectorySuffixAndExtension;
import ca.bc.gov.gbasites.load.common.LoadProviderSitesIntoGba;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.convert.AbstractSiteConverter;
import ca.bc.gov.gbasites.load.convert.SiteConverterAddress;
import ca.bc.gov.gbasites.load.convert.SiteConverterCKRD;
import ca.bc.gov.gbasites.load.convert.SiteConverterParts;
import ca.bc.gov.gbasites.load.merge.LoadEmergencyManagementSites;
import ca.bc.gov.gbasites.load.merge.MergeEmergencyManagementSites;
import ca.bc.gov.gbasites.load.merge.RecordMergeCounters;
import ca.bc.gov.gbasites.load.merge.SitePointMerger;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSiteConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSplitByProvider;
import ca.bc.gov.gbasites.load.readsource.SourceReaderArcGis;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFile;
import ca.bc.gov.gbasites.load.readsource.SourceReaderFileGdb;
import ca.bc.gov.gbasites.load.readsource.SourceReaderJoin;
import ca.bc.gov.gbasites.load.readsource.SourceReaderMapGuide;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.CollectionMap;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.gis.esri.gdb.file.FileGdbWriter;
import com.revolsys.io.Reader;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactory;
import com.revolsys.io.map.MapObjectFactoryRegistry;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.code.SimpleCodeTable;
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
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Counter;
import com.revolsys.util.Property;

public class ImportSites extends AbstractTaskByLocality implements SitePoint {

  public static final String PROVIDER = "Provider";

  public static final String PROVIDERS = "Providers";

  private static final String EM_READ = "EM READ";

  public static StatisticsDialog dialog;

  public static final DirectorySuffixAndExtension ERROR_BY_PROVIDER = new DirectorySuffixAndExtension(
    "ErrorByProvider", "_ERROR", ".tsv");

  private static final String FGDB_WRITE = "FGDB Write";

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

  public static final Map<String, String> siteTypeByBuildingType = new LinkedHashMap<>();

  public static final DirectorySuffixAndExtension SOURCE_BY_PROVIDER = new DirectorySuffixAndExtension(
    "SourceByProvider", "_SOURCE", ".tsv");

  public static final String PROVIDER_SPLIT = "P Split";

  public static final String ABC_SPLIT = "ABC Split";

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
          if (fieldName.startsWith("STREET_NAME") || fieldName.endsWith("_PARTNER_ORG_ID")) {
            final String newFieldName = fieldName.replace("_ID", "");
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING);
            }
          } else if (fieldName.equals(LOCALITY_ID)) {
            final String newFieldName = AbstractSiteConverter.LOCALITY_NAME;
            if (!sitePointRecordDefinition.hasField(newFieldName)) {
              recordDefinition.addField(newFieldName, DataTypes.STRING);
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

  private final CheckBox addressBcConvertCheckbox = new CheckBox("addressBcConvert", true);

  private final CheckBox addressBcDownloadCheckbox = new CheckBox("addressBcDownload", true);

  private final CheckBox addressBcSplitCheckbox = new CheckBox("addressBcSplit", true);

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

    final Runnable addressBcRunnable = () -> {
      final boolean download = this.addressBcDownloadCheckbox.isSelected();
      final boolean split = this.addressBcSplitCheckbox.isSelected();
      AddressBcSplitByProvider.split(this, download, split);
    };

    final boolean downloadData = this.providerDownloadCheckbox.isSelected();
    newConverterProcessNetwork("Downloading", downloadData, providerAction,
      "Download and Split Address BC", addressBcRunnable);
  }

  private void action2Convert() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      converter.convertData(this, true);
    };

    final Runnable addressBcRunnable = () -> {
      final boolean convert = this.addressBcConvertCheckbox.isSelected();
      if (convert) {
        AddressBcSiteConverter.convertAll(this, convert);
      }
    };

    final boolean convert = this.providerConvertCheckbox.isSelected();
    try {
      ProviderSitePointConverter.preProcess();
      newConverterProcessNetwork("Converting", convert, providerAction, "Convert Address BC",
        addressBcRunnable);
    } finally {
      ProviderSitePointConverter.postProcess(this.dataProvidersToProcess);
    }
  }

  private void addConverterProcesses(final String label, final ProcessNetwork processes,
    final Consumer<ProviderSitePointConverter> action) {
    if (!this.dataProvidersToProcess.isEmpty()) {
      final List<ProviderSitePointConverter> converters = Lists.toList(LinkedList::new,
        this.dataProvidersToProcess);
      for (int i = 0; i < getThreadCount(); i++) {
        processes.addProcess("Provider " + i, () -> {
          while (!isCancelled()) {
            try {
              final ProviderSitePointConverter converter = converters.remove(0);
              try {
                action.accept(converter);
              } catch (final Throwable e) {
                Logs.error(ProviderSitePointConverter.class,
                  "Error " + label + "\n" + converter.getPartnerOrganizationFileName(), e);
              }
            } catch (final IndexOutOfBoundsException e) {
              return;
            }
          }
        });
      }
    }
  }

  @Override
  protected void adjustSize() {
    final Rectangle bounds = SwingUtil.getScreenBounds();

    setLocation(bounds.x + 20, bounds.y + 20);

    final int width = Math.min(1500, bounds.width - 40);
    final int height = bounds.height - 40;
    setSize(width, height);
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    for (final String directory : Arrays.asList("AddressBc", PROVIDER)) {
      final Path baseDirectory = SITES_DIRECTORY.resolve(directory);
      NAME_ERROR_BY_PROVIDER.createDirectory(baseDirectory);
      ERROR_BY_PROVIDER.createDirectory(baseDirectory);
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
        // if (isProcessAllLocalities()) {
        // LoadProviderSitesIntoGba.writeNoSites();
        // }
      }
    }
    if (!isCancelled() && this.fgdbCheckbox.isSelected()) {
      setSelectedTab(ProviderSitePointConverter.LOCALITY);
      writeFgdb();
    }

    if (!isCancelled()) {
      writeCounts("PROVIDER_COUNTS.xlsx", PROVIDER);
      writeCounts("LOCALITY_COUNTS.xlsx", "Locality");
    }
    return !isCancelled();
  }

  public Identifier getPartnerOrganizationByShortName(final String shortName) {
    return this.partnerOrganizationIdByShortName.get(shortName);
  }

  private void initPartnerOrganizationShortNames() {
    final CodeTable partnerOrganizations = GbaController.getPartnerOrganizations();
    for (final Identifier partnerOrganizationId : partnerOrganizations.getIdentifiers()) {
      final String partnerOrganizationName = partnerOrganizations.getValue(partnerOrganizationId);
      String shortName = null;
      if (partnerOrganizationName.startsWith("Locality - ")) {
        shortName = partnerOrganizationName.substring(11);
      } else if (partnerOrganizationName.startsWith("Regional District - ")) {
        shortName = partnerOrganizationName.substring(20);
      } else if (partnerOrganizationName.startsWith("Provider - ")) {
        shortName = partnerOrganizationName.substring(11);
      } else {
        shortName = partnerOrganizationName;
      }
      if (shortName != null) {
        shortName = shortName.replace(' ', '_').toUpperCase();
        this.partnerOrganizationIdByShortName.put(shortName, partnerOrganizationId);
      }
    }
  }

  private void loadCodes() {
    loadCodes(SiteTables.FEATURE_STATUS_CODE);
    loadCodes(SiteTables.SITE_LOCATION_CODE);
    loadSiteTypeCodes();
  }

  private void loadCodes(final PathName pathName) {
    loadCodes(pathName, null);
  }

  private void loadCodes(final PathName pathName, final Consumer<Record> action) {
    final String typeName = pathName.getName();
    final RecordStore recordStore = GbaController.getUserRecordStore();
    final ClassPathResource resource = new ClassPathResource(
      "/ca/bc/gov/gba/schema/codes/" + typeName + ".tsv");
    try (
      Transaction transaction = recordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader.newRecordReader(resource)) {
      if (recordStore.getRecordDefinition(pathName) == null) {
        final SimpleCodeTable codeTable = new SimpleCodeTable(typeName);
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

  private void newConverterProcessNetwork(final String label, final boolean providerEnabled,
    final Consumer<ProviderSitePointConverter> action, final String addressBcProcessName,
    final Runnable addressBcRunnable) {
    final ProcessNetwork processes = new ProcessNetwork();
    if (providerEnabled) {
      addConverterProcesses(label, processes, action);
    }
    processes.addProcess(addressBcProcessName, addressBcRunnable);
    setSelectedTab(PROVIDER);
    processes.startAndWait();
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new SitePointMerger(this);
  }

  @Override
  protected BasePanel newPanelOptions() {
    return new BasePanel(//
      newPanelOptionsAddressBc(), //
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
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Address BC"));
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
    newLabelCountTableModel(PROVIDER, //
      "Data Provider", //
      "P Source", //
      "ABC Source", //
      "P Converted", //
      "ABC Converted", //
      "P Ignored", //
      "ABC Ignored", //
      "P Error", //
      "ABC Error", //
      "P Warning", //
      "ABC Warning" //
    );

    final String PROVIDER_READ = "P Read";
    final String ABC_READ = "ABC Read";

    final String PROVIDER_USED = "P Used";
    final String ABC_USED = "ABC Used";
    final String PROVIDER_ABC_TOTAL = "Provider & ABC";

    final LabelCountMapTableModel localityCounts = newLabelCountTableModel(
      ProviderSitePointConverter.LOCALITY, //
      ProviderSitePointConverter.LOCALITY, //

      PROVIDER_READ, //
      PROVIDER_USED, //
      ABC_READ, //
      ABC_USED, //
      PROVIDER_ABC_TOTAL, //

      MERGED_READ, //

      MATCHED, //
      INSERTED, //
      UPDATED, //
      DELETED, //
      LoadProviderSitesIntoGba.TO_DELETE, //
      MERGED_WRITE, //

      EM_READ, //
      FGDB_WRITE //
    );

    final LabelCountMapTableModel providersCounters = RecordMergeCounters.addProviderCounts(this,
      PROVIDERS);
    final LabelCountMapTableModel addressBcCounters = RecordMergeCounters.addProviderCounts(this,
      "Address BC");

    localityCounts.addTotalColumn(PROVIDER_READ) //
      .addCounters(providersCounters.getLabelCountMap(READ)) //
    ;
    localityCounts.addTotalColumn(PROVIDER_USED) //
      .addCounters(providersCounters.getLabelCountMap(RecordMergeCounters.USED)) //
    ;
    localityCounts.addTotalColumn(ABC_READ) //
      .addCounters(addressBcCounters.getLabelCountMap(READ)) //
    ;
    localityCounts.addTotalColumn(ABC_USED) //
      .addCounters(addressBcCounters.getLabelCountMap(RecordMergeCounters.USED)) //
    ;
    localityCounts.addTotalColumn(PROVIDER_ABC_TOTAL, PROVIDER_USED, ABC_USED);

    for (final String localityName : getLocalityNamesToProcess()) {
      localityCounts.addRowLabel(localityName);
    }
    newLabelCountTableModel(MergeEmergencyManagementSites.EM_SITES, "Type Name", GBA_READ,
      LoadProviderSitesIntoGba.PROVIDER_READ, MergeEmergencyManagementSites.IGNORE_XCOVER, INSERTED,
      UPDATED);
    if (hasProviders) {
      newLabelCountTableModel(ERROR, //
        "Message", //
        ERROR//
      );

      newLabelCountTableModel(ProviderSitePointConverter.NAME_ERRORS, //
        "Message", //
        ERROR//
      );

      newLabelCountTableModel(ProviderSitePointConverter.WARNING, "Message", //
        ProviderSitePointConverter.WARNING);
    }
  }

  private void writeCounts(final String fileName, final String category) {
    final Path providerCountsPath = SITES_DIRECTORY.resolve(fileName);
    final LabelCountMapTableModel providerCounts = this.getLabelCountTableModel(category);
    providerCounts.writeCounts(providerCountsPath);
  }

  private void writeFgdb() {
    try (
      AtomicPathUpdator pathUpdator = new AtomicPathUpdator(this, SITES_DIRECTORY,
        "SITE_POINT.gdb");
      FileGdbRecordStore recordStore = FileGdbRecordStoreFactory
        .newRecordStore(pathUpdator.getPath());) {
      RecordDefinitionImpl recordDefinition = getSitePointTsvRecordDefinition();
      recordDefinition = new RecordDefinitionImpl(recordDefinition);
      for (final FieldDefinition field : recordDefinition.getFields()) {
        field.setRequired(false);
      }
      recordDefinition.setIdFieldName(null);
      recordDefinition = recordStore.getRecordDefinition(recordDefinition);
      try (
        FileGdbWriter writer = recordStore.newRecordWriter(recordDefinition)) {

        final CollectionMap<String, Record, List<Record>> emergencyManagementSitesByLocality = new LoadEmergencyManagementSites()
          .loadEmergencyManagementSites(this);

        final Collection<String> localityNames = GbaController.getLocalities().getBoundaryNames();
        for (final String localityName : cancellable(localityNames)) {
          final String localityFileName = BatchUpdateDialog.toFileName(localityName);
          final Counter writeCounter = getCounter("Locality", localityName, FGDB_WRITE);

          final List<Record> emSites = emergencyManagementSitesByLocality.getOrEmpty(localityName);
          if (!emSites.isEmpty()) {
            final Counter emCounter = getCounter("Locality", localityName, EM_READ);
            for (final Record record : emSites) {
              emCounter.add();
              final Record writeRecord = writer.newRecord(record);
              writer.write(writeRecord);
              writeCounter.add();
            }
          }

          final Path localityFile = SITE_POINT.getLocalityFilePath(SITES_DIRECTORY,
            localityFileName);
          try (
            RecordReader reader = RecordReader.newRecordReader(localityFile)) {
            for (final Record record : cancellable(reader)) {
              final Record writeRecord = writer.newRecord(record);
              writer.write(writeRecord);
              writeCounter.add();
            }
          }
        }
      }
    }
  }

}
