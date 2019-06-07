package ca.bc.gov.gbasites.load.provider.other;

import java.nio.file.Path;
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
import ca.bc.gov.gbasites.load.common.LoadEmergencyManagementSites;
import ca.bc.gov.gbasites.load.common.LoadProviderSitesIntoGba;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.converter.SiteConverterAddress;
import ca.bc.gov.gbasites.load.converter.SiteConverterParts;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcConvert;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBcSplitByProvider;
import ca.bc.gov.gbasites.load.provider.ckrd.SiteConverterCKRD;
import ca.bc.gov.gbasites.load.provider.nanaimo.SiteConverterNanaimo;
import ca.bc.gov.gbasites.load.sourcereader.SourceReaderArcGis;
import ca.bc.gov.gbasites.load.sourcereader.SourceReaderFile;
import ca.bc.gov.gbasites.load.sourcereader.SourceReaderFileGdb;
import ca.bc.gov.gbasites.load.sourcereader.SourceReaderJoin;
import ca.bc.gov.gbasites.load.sourcereader.SourceReaderMapGuide;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.collection.list.Lists;
import com.revolsys.io.Reader;
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
  public static final ThreadLocal<Identifier> custodianPartnerOrgIdForThread = new ThreadLocal<>();

  public static StatisticsDialog dialog;

  public static final Path LOCALITY_DIRECTORY = GbaController.getDataDirectory("Sites/Locality");

  private static final long serialVersionUID = 1L;

  public static final Path SITES_DIRECTORY = GbaController.getDataDirectory("Sites");

  public static final Map<String, String> siteTypeByBuildingType = new LinkedHashMap<>();

  public static void initializeService() {

    MapObjectFactoryRegistry.newFactory("gbaSiteConverterAddress", SiteConverterAddress::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterCKRD", SiteConverterCKRD::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterNanaimo", SiteConverterNanaimo::new);
    MapObjectFactoryRegistry.newFactory("gbaSiteConverterParts", SiteConverterParts::new);

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

  private CheckBox loadSitesGdb;

  private final Map<String, Identifier> partnerOrganizationIdByShortName = new HashMap<>();

  private ComboBox<ProviderSitePointConverter> providerComboBox;

  private final CheckBox providerConvertCheckbox = new CheckBox("providerConvertCheckbox", true);

  private final CheckBox providerDownloadCheckbox = new CheckBox("providerDownload", true);

  private final int threadCount = 10;

  public ImportSites() {
    super("Import Sites");
    dialog = this;
    setLockName(null);
    setUseTotalCounts(false);
    setLogChanges(false);

    ProviderSitePointConverter.init();
  }

  private void action1Download() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      final boolean downloadData = this.providerDownloadCheckbox.isSelected();
      converter.downloadData(this, downloadData);
    };

    final Runnable addressBcRunnable = () -> {
      final boolean download = this.addressBcDownloadCheckbox.isSelected();
      final boolean split = this.addressBcSplitCheckbox.isSelected();
      AddressBcSplitByProvider.split(dialog, download, split);
    };

    newConverterProcessNetwork(providerAction, "Download and Split Address BC", addressBcRunnable);
  }

  private void action2Convert() {
    final Consumer<ProviderSitePointConverter> providerAction = converter -> {
      final boolean convert = this.providerConvertCheckbox.isSelected();
      converter.convertData(this, convert);
    };

    final Runnable addressBcRunnable = () -> {
      if (this.addressBcConvertCheckbox.isSelected()) {
        new AddressBcConvert(this).run();
      }
    };

    newConverterProcessNetwork(providerAction, "Convert Address BC", addressBcRunnable);
  }

  private void addConverterProcesses(final ProcessNetwork processes,
    final Consumer<ProviderSitePointConverter> action) {
    if (!this.dataProvidersToProcess.isEmpty()) {
      final List<ProviderSitePointConverter> converters = Lists.toList(LinkedList::new,
        this.dataProvidersToProcess);
      for (int i = 0; i < this.threadCount; i++) {
        processes.addProcess("Provider " + i, () -> {
          while (!isCancelled()) {
            try {
              final ProviderSitePointConverter converter = converters.remove(0);
              action.accept(converter);
            } catch (final Throwable e) {
              return;
            }
          }
        });
      }
    }
  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    loadFeatureStatusCodes();
    loadSiteLocationCodes();
    loadSiteTypeCodes();

    action1Download();
    action2Convert();

    initPartnerOrganizationShortNames();
    if (!isCancelled()) {
      if (isHasLocalitiesToProcess()) {
        // setSelectedTab(ProviderSitePointConverter.LOCALITY);
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

  private void newConverterProcessNetwork(final Consumer<ProviderSitePointConverter> action,
    final String addressBcProcessName, final Runnable addressBcRunnable) {
    final ProcessNetwork processes = new ProcessNetwork();
    addConverterProcesses(processes, action);
    processes.addProcess(addressBcProcessName, addressBcRunnable);
    setSelectedTab("Provider");
    try {
      ProviderSitePointConverter.preProcess();
      processes.startAndWait();
    } finally {
      ProviderSitePointConverter.postProcess(this.dataProvidersToProcess);
    }
  }

  @Override
  protected Consumer<Identifier> newLocalityHandler() {
    return new LoadProviderSitesIntoGba(this);
  }

  @Override
  protected BasePanel newPanelOptions() {
    return new BasePanel(//
      newPanelOptionsAddressBc(), //
      newPanelOptionsProvider(), //
      newPanelOptionsLocalities() //
    );
  }

  private BasePanel newPanelOptionsAddressBc() {

    final BasePanel downloadPanel = new BasePanel(//
      SwingUtil.newLabel("Download"), //
      this.addressBcDownloadCheckbox, //
      SwingUtil.newLabel("Split"), //
      this.addressBcSplitCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.addressBcConvertCheckbox //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Address BC"));
    GroupLayouts.makeColumns(downloadPanel, 6, true);
    return downloadPanel;
  }

  private BasePanel newPanelOptionsLocalities() {
    this.loadSitesGdb = new CheckBox("loadSitesGdb", false);

    final BasePanel optionsPanel = super.newPanelOptions();
    optionsPanel.add(SwingUtil.newLabel("Load sites.gdb"));
    optionsPanel.add(this.loadSitesGdb);
    GroupLayouts.makeColumns(optionsPanel, 2, true);
    optionsPanel.setBorder(BorderFactory.createTitledBorder("Import Sites into GBA Database"));
    return optionsPanel;
  }

  private BasePanel newPanelOptionsProvider() {
    final List<ProviderSitePointConverter> dataProviders = new ArrayList<>();
    dataProviders.add(null);
    dataProviders.addAll(ProviderSitePointConverter.getLoaders());
    this.providerComboBox = ComboBox.newComboBox("dataProvider", dataProviders);

    final BasePanel downloadPanel = new BasePanel(//
      SwingUtil.newLabel("Data Provider"), //
      this.providerComboBox, //
      SwingUtil.newLabel("Download"), //
      this.providerDownloadCheckbox, //
      SwingUtil.newLabel("Convert"), //
      this.providerConvertCheckbox //
    );
    downloadPanel.setBorder(BorderFactory.createTitledBorder("Provider: Download and Convert"));
    GroupLayouts.makeColumns(downloadPanel, 2, true);
    return downloadPanel;
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
    newLabelCountTableModel("Provider", //
      ProviderSitePointConverter.DATA_PROVIDER, //
      "Provider Download", //
      "Address BC Download", //
      "Provider Convert", //
      "Address BC Convert" //
    );
    if (hasProviders) {
      if (this.providerConvertCheckbox.isSelected()) {
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
