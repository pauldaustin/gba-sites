package ca.bc.gov.gbasites.load.common;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.swing.BorderFactory;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.process.AbstractTaskByLocality;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.load.common.converter.SiteConverterAddress;
import ca.bc.gov.gbasites.load.common.converter.SiteConverterParts;
import ca.bc.gov.gbasites.load.provider.ckrd.SiteConverterCKRD;
import ca.bc.gov.gbasites.load.provider.nanaimo.SiteConverterNanaimo;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.io.Reader;
import com.revolsys.io.file.Paths;
import com.revolsys.io.map.MapObjectFactoryRegistry;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.spring.resource.ClassPathResource;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.component.BasePanel;
import com.revolsys.swing.field.CheckBox;
import com.revolsys.swing.field.ComboBox;
import com.revolsys.swing.layout.GroupLayouts;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Property;

public class ImportSites extends AbstractTaskByLocality implements SitePoint {
  public static StatisticsDialog dialog;

  public static final Path LOCALITY_DIRECTORY = GbaController.getDataDirectory("Sites/Locality");

  private static final long serialVersionUID = 1L;

  public static final Path SITES_DIRECTORY = GbaController.getDataDirectory("Sites");

  private static CheckBox useCachedFiles;

  public static final Map<String, String> siteTypeByBuildingType = new LinkedHashMap<>();

  public static final ThreadLocal<Identifier> custodianPartnerOrgIdForThread = new ThreadLocal<>();

  private static CheckBox processAllProviders;

  public static void initializeService() {

    MapObjectFactoryRegistry.newFactory("gbaSiteConverterAddress", "gbaSiteConverterAddress",
      SiteConverterAddress::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterCKRD", "gbaSiteConverterSiteConverterCKRD",
      SiteConverterCKRD::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterNanaimo",
      "gbaSiteConverterSiteConverterNanaimo", SiteConverterNanaimo::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterParts", "gbaSiteConverterParts",
      SiteConverterParts::new);

    MapObjectFactoryRegistry.newFactory("gbaSiteLoaderArcGis", "gbaSiteLoaderArcGis",
      ProviderSitePointConverter::newProviderSitePointConverterArcGis);
    MapObjectFactoryRegistry.newFactory("gbaSiteLoaderMapGuide", "gbaSiteLoaderMapGuide",
      ProviderSitePointConverter::newProviderSitePointConverterMapGuide);
    MapObjectFactoryRegistry.newFactory("gbaSiteLoaderFile", "gbaSiteLoaderFile",
      ProviderSitePointConverter::newProviderSitePointConverterFile);
    MapObjectFactoryRegistry.newFactory("gbaSiteLoaderFileGdb", "gbaSiteLoaderFileGdb",
      ProviderSitePointConverter::newProviderSitePointConverterFileGdb);
    MapObjectFactoryRegistry.newFactory("gbaSiteLoaderJoin", "gbaSiteLoaderJoin",
      ProviderSitePointConverter::newProviderSitePointConverterJoin);
  }

  public static boolean isDownloadData() {
    return !useCachedFiles.isSelected();
  }

  public static boolean isProcessAllProviders() {
    return processAllProviders.isSelected();
  }

  public static void main(final String[] args) {
    initializeService();
    start(ImportSites.class);
  }

  private final Map<String, Identifier> partnerOrganizationIdByShortName = new HashMap<>();

  private final List<ProviderSitePointConverter> dataProvidersToProcess = Collections
    .synchronizedList(new LinkedList<>());

  private final int threadCount = 10;

  private ComboBox<ProviderSitePointConverter> dataProviderComboBox;

  private CheckBox loadSitesGdb;

  public ImportSites() {
    super("Import Sites");
    dialog = this;
    setLockName(null);
    setUseTotalCounts(false);
    setLogChanges(false);

    ProviderSitePointConverter.init();
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    loadFeatureStatusCodes();
    loadSiteLocationCodes();
    loadSiteTypeCodes();
    processDataProviderDownload();
    initPartnerOrganizationShortNames();
    if (!isCancelled()) {
      if (isHasLocalitiesToProcess()) {
        setSelectedTab(ProviderSitePointConverter.LOCALITY);
        super.batchUpdate(transaction);
        if (isProcessAllLocalities()) {
          LoadProviderSitesIntoGba.writeNoSites();
        }
      }
    }
    if (!isCancelled() && this.loadSitesGdb.isSelected()) {
      final LoadEmergencyManagementSites loadEmergencyManagementSites = new LoadEmergencyManagementSites(
        this);
      loadEmergencyManagementSites.run();
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

  private void loadFeatureStatusCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader.newRecordReader(
        new ClassPathResource("/ca/bc/gov/gba/schema/codes/FEATURE_STATUS_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString("FEATURE_STATUS_CODE");
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.FEATURE_STATUS_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.FEATURE_STATUS_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
      }
    }
  }

  private void loadSiteLocationCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader.newRecordReader(
        new ClassPathResource("/ca/bc/gov/gba/schema/codes/SITE_LOCATION_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString("SITE_LOCATION_CODE");
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.SITE_LOCATION_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.SITE_LOCATION_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
      }
    }
  }

  private void loadSiteTypeCodes() {
    final RecordStore gbaRecordStore = GbaController.getUserRecordStore();
    try (
      Transaction transaction = gbaRecordStore.newTransaction(Propagation.REQUIRES_NEW);
      Reader<Record> reader = RecordReader
        .newRecordReader(new ClassPathResource("/ca/bc/gov/gba/schema/codes/SITE_TYPE_CODE.tsv"))) {
      for (final Record record : reader) {
        final String code = record.getString(SITE_TYPE_CODE);
        final String description = record.getString("DESCRIPTION");
        if (gbaRecordStore.getRecord(SiteTables.SITE_TYPE_CODE, code) == null) {
          final Record codeRecord = gbaRecordStore.newRecord(SiteTables.SITE_TYPE_CODE);
          codeRecord.setIdentifier(Identifier.newIdentifier(code));
          codeRecord.setValue("DESCRIPTION", description);
          final Identifier integrationSessionId = getIntegrationSessionId();
          codeRecord.setValue(CREATE_INTEGRATION_SESSION_ID, integrationSessionId);
          codeRecord.setValue(MODIFY_INTEGRATION_SESSION_ID, integrationSessionId);
          gbaRecordStore.insertRecord(codeRecord);
        }
        final String plGroup = record.getString("PL_GROUP");
        if (Property.hasValue(plGroup)) {
          final String plType = record.getString("PL_TYPE");
          for (final String type : plType.split(",")) {
            siteTypeByBuildingType.put(plGroup + "-" + type, code);
          }
        }
      }
    }
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new LoadProviderSitesIntoGba(this);
  }

  private BasePanel newPanelDownload() {
    final List<ProviderSitePointConverter> dataProviders = new ArrayList<>();
    dataProviders.add(null);
    dataProviders.addAll(ProviderSitePointConverter.getLoaders());
    this.dataProviderComboBox = ComboBox.newComboBox("dataProvider", dataProviders);
    useCachedFiles = new CheckBox("useCachedFiles", false);
    processAllProviders = new CheckBox("processAll", false);

    final BasePanel downloadPanel = new BasePanel(//
      SwingUtil.newLabel("All Providers"), //
      processAllProviders, //
      SwingUtil.newLabel("Data Provider"), //
      this.dataProviderComboBox, //
      SwingUtil.newLabel("Use Cached Files"), //
      useCachedFiles //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Download data From Providers"));
    GroupLayouts.makeColumns(downloadPanel, 2, true);
    return downloadPanel;
  }

  private BasePanel newPanelLocalities() {
    this.loadSitesGdb = new CheckBox("loadSitesGdb", false);

    final BasePanel optionsPanel = super.newPanelOptions();
    optionsPanel.add(SwingUtil.newLabel("Load sites.gdb"));
    optionsPanel.add(this.loadSitesGdb);
    GroupLayouts.makeColumns(optionsPanel, 2, true);
    optionsPanel.setBorder(BorderFactory.createTitledBorder("Import Sites into GBA Database"));
    return optionsPanel;
  }

  @Override
  protected BasePanel newPanelOptions() {
    return new BasePanel(//
      newPanelDownload(), //
      newPanelLocalities() //
    );
  }

  @Override
  protected void preUpdateRecord(final Record record) {
    final Identifier custodianPartnerOrgId = custodianPartnerOrgIdForThread.get();
    if (custodianPartnerOrgId != null) {
      if (record.equalValue(CUSTODIAN_PARTNER_ORG_ID, custodianPartnerOrgId)) {
        record.setValue(CUSTODIAN_SESSION_ID, getIntegrationSessionId());
      }
    }
  }

  private void processDataProviderDownload() {
    if (!this.dataProvidersToProcess.isEmpty()) {
      setSelectedTab(ProviderSitePointConverter.DATA_PROVIDER);
      try {
        final ProcessNetwork processes = new ProcessNetwork();

        ProviderSitePointConverter.preProcess();

        for (int i = 0; i < this.threadCount; i++) {
          processes.addProcess("Provider " + i, () -> {
            while (!isCancelled()) {
              ProviderSitePointConverter loader;
              try {
                loader = this.dataProvidersToProcess.remove(0);
              } catch (final Throwable e) {
                return;
              }
              loader.run();
            }
          });
        }
        processes.startAndWait();
        if (!isCancelled()) {

          final Path providerPath = SITES_DIRECTORY.resolve("Provider");
          final Path addressBcPath = providerPath.resolve("_ADDRESS_BC");
          if (Paths.exists(addressBcPath)) {
            try {
              Files.walkFileTree(addressBcPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
                  throws IOException {
                  final String fileName = Paths.getFileName(file);
                  if (!fileName.startsWith(".")) {
                    final Path relativePath = addressBcPath.relativize(file);
                    final Path providerFilePath = providerPath.resolve(relativePath);
                    Paths.createParentDirectories(providerFilePath);
                    Files.move(file, providerFilePath, StandardCopyOption.ATOMIC_MOVE);
                  }
                  return FileVisitResult.CONTINUE;
                }
              });
            } catch (final IOException e) {
            }
          }
          Paths.deleteDirectories(addressBcPath);
        }
      } finally {
        ProviderSitePointConverter.postProcess();
      }
    }
  }

  @Override
  protected void setOptions(final BasePanel optionsPanel) {
    super.setOptions(optionsPanel);
    final ProviderSitePointConverter dataProviderToProcess = this.dataProviderComboBox
      .getSelectedItem();

    if (isProcessAllProviders()) {
      this.dataProvidersToProcess.addAll(ProviderSitePointConverter.getLoaders());
    } else if (dataProviderToProcess != null) {
      this.dataProvidersToProcess.add(dataProviderToProcess);
    }
    final boolean hasProviders = !this.dataProvidersToProcess.isEmpty();
    if (hasProviders) {
      newLabelCountTableModel(ProviderSitePointConverter.DATA_PROVIDER,
        ProviderSitePointConverter.DATA_PROVIDER, //
        READ, //
        WRITE, //
        ERROR, //
        ProviderSitePointConverter.WARNING, //
        ProviderSitePointConverter.SECONDARY, //
        ProviderSitePointConverter.IGNORED, //
        ProviderSitePointConverter.READ_ADDRESS_BC, //
        ProviderSitePointConverter.IGNORE_ADDRESS_BC//
      );
    }
    if (hasProviders) {
      if (isHasLocalitiesToProcess()) {
        newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.PROVIDER_CONVERT, //
          GBA_READ, //
          LoadProviderSitesIntoGba.PROVIDER_READ, //
          LoadProviderSitesIntoGba.MATCHED, //
          INSERTED, //
          UPDATED, //
          LoadProviderSitesIntoGba.TO_DELETE //
        );
      } else {
        newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.LOCALITY, //
          ProviderSitePointConverter.PROVIDER_CONVERT //
        );
      }
      for (final String localityName : getLocalityNamesToProcess()) {
        newLabelCount(ProviderSitePointConverter.LOCALITY, localityName,
          ProviderSitePointConverter.PROVIDER_CONVERT);
      }
    } else if (isHasLocalitiesToProcess()) {
      newLabelCountTableModel(ProviderSitePointConverter.LOCALITY, //
        ProviderSitePointConverter.LOCALITY, //
        GBA_READ, //
        LoadProviderSitesIntoGba.PROVIDER_READ, //
        LoadProviderSitesIntoGba.MATCHED, //
        INSERTED, //
        UPDATED, //
        LoadProviderSitesIntoGba.TO_DELETE //
      );
      for (final String localityName : getLocalityNamesToProcess()) {
        newLabelCount(ProviderSitePointConverter.LOCALITY, localityName,
          LoadProviderSitesIntoGba.PROVIDER_READ);
      }
    }
    if (this.loadSitesGdb.isSelected()) {
      newLabelCountTableModel(LoadEmergencyManagementSites.EM_SITES, "Type Name", GBA_READ,
        LoadProviderSitesIntoGba.PROVIDER_READ, LoadEmergencyManagementSites.IGNORE_XCOVER,
        INSERTED, UPDATED);
    }
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
}
