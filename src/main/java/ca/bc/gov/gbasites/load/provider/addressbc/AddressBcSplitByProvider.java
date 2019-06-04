package ca.bc.gov.gbasites.load.provider.addressbc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.type.code.PartnerOrganization;
import ca.bc.gov.gba.model.type.code.PartnerOrganizations;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.collection.range.RangeSet;
import com.revolsys.collection.set.Sets;
import com.revolsys.io.Reader;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.io.format.json.Json;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Property;

public class AddressBcSplitByProvider implements Cancellable, SitePoint {

  private class ProviderWriter {

    private final PartnerOrganization partnerOrganization;

    private final RecordWriter recordWriter;

    private int writeCount = 0;

    private final Path targetPath;

    public ProviderWriter(final PartnerOrganization partnerOrganization,
      final RecordDefinitionProxy recordDefinition) {
      this.partnerOrganization = partnerOrganization;
      final String shortName = partnerOrganization.getPartnerOrganizationShortName();
      final String baseName = BatchUpdateDialog.toFileName(shortName) + "_ADDRESS_BC";
      final String fileName = baseName + ".tsv";
      this.targetPath = AddressBcSplitByProvider.this.inputByProviderDirectory.resolve(fileName);
      this.recordWriter = RecordWriter.newRecordWriter(recordDefinition, this.targetPath);
      this.recordWriter.setProperty("useQuotes", false);
      Paths.deleteDirectories(
        AddressBcSplitByProvider.this.inputByProviderDirectory.resolve("_" + baseName + ".prj"));
    }

    void close() {
      this.recordWriter.close();
      if (this.writeCount == 0) {
        try {
          Files.deleteIfExists(this.targetPath);
        } catch (final IOException e) {
          Logs.error(this, "Unable to delete: " + this.targetPath, e);
        }
      }
    }

    public void writeRecord(final Record record) {
      AddressBcSplitByProvider.this.importSites.addLabelCount(SPLIT, this.partnerOrganization,
        BatchUpdateDialog.WRITE);
      this.recordWriter.write(record);
      this.writeCount++;
    }
  }

  private static final String SPLIT = "Split";

  private static final Map<String, String> ADDRESS_BC_PROVIDER_ALIAS = Maps
    .<String, String> buildHash() //
    .add("CENTRAL COAST REGIONAL DISTRICT", "CCRD") //
    .add("CENTRAL COAST", "CCRD") //
    .add("COWICHAN VALLEY", "CVRD") //
    .add("GREATER VANCOUVER REGIONAL DISTRICT", "MVRD") //
    .add("GVRD", "MVRD") //
    .add("CITY OF KIMBERLEY", "Kimberley") //
    .add("MOUNT WADDINGTON REGIONAL DISTRICT", "MWRD") //
    .add("MOUNT WADDINGTON", "MWRD") //
    .add("NORTHERN ROCKIES REGIONAL MUNICIPALITY", "NRRM")//
    .add("NORTHERN ROCKIES", "NRRM")//
    .add("NORTH OKANAGAN REGIONAL DISTRICT", "NORD")
    .add("NORTH OKANAGAN", "NORD")
    .add("NORTH COWICHAN", "CVRD")
    .add("TOWN OF VIEW ROYAL", "View Royal") //
    .add("POWELL RIVER CITY", "Powell River") //
    .add("SQUAMISH LILLOOET RD RURAL", "SLRD") //
    .add("TOWN OF LADYSMITH", "Ladysmith") //
    .add("STSAILES", "Chehalis") //
    .add("VILLAGE OF FRUITVALE", "Fruitvale") //
    .add("VILLAGE OF MIDWAY", "Midway") //
    .add("VILLAGE OF WARFIELD", "Warfield") //
    .add("NORTHERN ROCKIES", "NRRM") //
    .getMap();

  private final Path inputDirectory;

  private final Path inputByProviderDirectory;

  private final Map<String, RangeSet> unitDescriptorsById = new HashMap<>();

  private final Map<String, String> buildingNameById = new HashMap<>();

  private final Map<String, String> buildingTypeById = new HashMap<>();

  private final Map<String, String> postalCodeById = new HashMap<>();

  private final AddressBcImportSites importSites;

  private final Map<String, ProviderWriter> writerByProvider = new HashMap<>();

  private final List<ProviderWriter> writers = new ArrayList<>();

  private TsvWriter extraDataWriter;

  private RecordDefinitionImpl recordDefinition;

  public AddressBcSplitByProvider(final AddressBcImportSites importSites) {
    this.importSites = importSites;
    this.inputDirectory = importSites.getInputDirectory();
    this.inputByProviderDirectory = importSites.getInputByProviderDirectory();
    Paths.deleteDirectories(this.inputByProviderDirectory);
    Paths.createDirectories(this.inputByProviderDirectory);
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  private void loadConfig() {
    final Path SITE_CONFIG_DIRECTORY = GbaController.getGbaPath().resolve("etc/Sites");

    final Path siteProviderConfigPath = SITE_CONFIG_DIRECTORY.resolve("Provider");
    if (Paths.exists(siteProviderConfigPath)) {
      try {
        Files.list(siteProviderConfigPath).forEach(path -> {
          try {
            final String fileNameExtension = Paths.getFileNameExtension(path);
            if ("json".equals(fileNameExtension)) {
              final MapEx providerConfig = Json.toMap(path);
              final String dataProvider = providerConfig.getString("dataProvider");
              final String dataProviderUpper = dataProvider.toUpperCase();
              ProviderWriter providerWriter = this.writerByProvider.get(dataProviderUpper);
              if (providerWriter == null) {
                final PartnerOrganization partnerOrganization = PartnerOrganizations
                  .newPartnerOrganization(dataProvider);
                providerWriter = new ProviderWriter(partnerOrganization, this.recordDefinition);
                this.writers.add(providerWriter);
                this.writerByProvider.put(dataProvider, providerWriter);
                this.writerByProvider.put(dataProviderUpper, providerWriter);
              }

              final List<String> issuingAgencies = providerConfig.getValue("issuingAgencies");
              if (issuingAgencies != null) {
                for (final String issuingAgency : issuingAgencies) {
                  this.writerByProvider.put(issuingAgency, providerWriter);
                  ADDRESS_BC_PROVIDER_ALIAS.put(issuingAgency, dataProvider);
                  final String issuingAgencyUpper = issuingAgency.toUpperCase();
                  this.writerByProvider.put(issuingAgencyUpper, providerWriter);
                  ADDRESS_BC_PROVIDER_ALIAS.put(issuingAgencyUpper, dataProvider);
                }
              }
            }
          } catch (final Throwable e) {
            Logs.error(this, "Unable to load config:" + path, e);
          }
        });
      } catch (final Throwable e) {
        Logs.error(this, "Unable to load config:" + siteProviderConfigPath, e);
      }
    }

  }

  private void loadExtendedAddress() {
    final Set<String> newBuildingTypes = new TreeSet<>();
    final MapEx buildingTypeMap = new LinkedHashMapEx() //
      .add("CHURCH", "Civic Chruch") //
      .add("COMMERCIAL", "Commercial Commercial") //
      .add("COMMUNITY", "Civic Community Hall") //
      .add("Commercial Commercial", "Commercial Commercial") //
      .add("FARM BUILD", "Farm Building") //
      .add("FIRE", "Fire Hall") //
      .add("GOLF", "Recreation Golf") //
      .add("GOVERNMENT", "Civic Government") //
      .add("Hospital Care", "Hospital Care") //
      .add("Hotel Hostel", "Hotel Hostel") //
      .add("INDUSTRIAL", "Commercial Industrial") //
      .add("LIBRARY", "Civic Library") //
      .add("MOSQUE", "Civic Mosque") //
      .add("MULTI FAMI", "Residential Condo") //
      .add("MUNICIPAL", "Government Municipal") //
      .add("MUSEUM", "Civic Museum") //
      .add("PARK BUILD", "Park Building") //
      .add("RECREATION", "Recreation Recreation") //
      .add("RECYCLING", "Civic Recycling") //
      .add("Residential Condo", "Residential Condo") //
      .add("SCHOOL", "School Standard") //
      .add("SINGLE FAM", "Residential Single Family") //
      .add("SKI", "Recreation Ski") //
      .add("STABLES", "Farm Stables") //
    ;
    final Set<String> ignoreBuildingTypes = Sets.newHash("MIXED USE", "OTHER");

    final Pattern postalCodePattern = Pattern.compile("[A-Z]\\d[A-Z] ?\\d[A-Z]\\d");

    final List<String> ignoreAddressNotes = Arrays.asList("LSCR", "PC NOT ON CANADAPOST YET",
      "CHECK POSTAL CODE", "CHECK PC", "CHECK PC - BOGUS", "<Null>",
      "CHECK POSTAL CODE - RESIDENTIAL", "CHECK POSTAL CODE - COMMERCIAL", "Bldg plan 127 suites",
      "97 unit rental building", "check pc b/c new units", "CHECK PC - commercial unit");

    final Path file = this.inputDirectory.resolve("ABC_EXTENDED_ADDRESS.csv");
    try (
      Reader<Record> reader = RecordReader.newRecordReader(file)) {
      for (final Record record : reader) {
        final String civicId = record.getValue(AddressBc.CIVIC_ID);

        String postalCode = record.getValue(AddressBc.POSTAL_CODE);
        if (Property.hasValue(postalCode)) {
          postalCode = postalCode.toUpperCase();
          if (postalCodePattern.matcher(postalCode).matches()) {
            if (postalCode.length() == 6) {
              postalCode = postalCode.substring(0, 3) + " " + postalCode.substring(3);
            }
            this.postalCodeById.put(civicId, postalCode);
          } else {
            writeExtraDataWarning("ABC_EXTENDED_ADDRESS.csv", civicId, AddressBc.POSTAL_CODE,
              postalCode);
          }
        }

        String buildingType = record.getValue(AddressBc.BUILDING_TYPE);
        if (!Property.hasValue(buildingType)) {
          final String addressNotes = record.getValue("ADDRESS_NOTES");
          if (Property.hasValue(addressNotes)) {
            if (addressNotes.startsWith("Hostel")) {
              buildingType = "Hotel Hostel";
            } else if (addressNotes.equalsIgnoreCase("commercial unit")) {
              buildingType = "Commercial Commercial";
            } else if (addressNotes.equalsIgnoreCase("Commercial")) {
              buildingType = "Commercial Commercial";
            } else if (addressNotes.equalsIgnoreCase("residential unit")) {
              buildingType = "Residential Condo";
            } else if (addressNotes.equalsIgnoreCase("Care Facility")) {
              buildingType = "Hospital Care";
            } else if (!ignoreAddressNotes.contains(addressNotes)) {
              System.err.println(addressNotes);
            }
          }

        }
        if (buildingType != null && !ignoreBuildingTypes.contains(buildingType)) {
          String code = buildingTypeMap.getString(buildingType);
          if (code == null) {
            code = buildingTypeMap.getString(buildingType.toUpperCase().trim());
          }

          if (code == null) {
            newBuildingTypes.add(buildingType.toUpperCase());
          } else {
            this.buildingTypeById.put(civicId, buildingType);

          }
        }
        final String buildingName = record.getValue(AddressBc.BUILDING_NAME);
        if (Property.hasValue(buildingName)) {
          this.buildingNameById.put(civicId, buildingName);
        }

      }
    }
    for (final String buildingType : newBuildingTypes) {
      writeExtraDataWarning("ABC_EXTENDED_ADDRESS.csv", null, AddressBc.BUILDING_TYPE,
        buildingType);
    }
    System.out.println("Postal Code Count\t" + this.postalCodeById.size());
  }

  private void loadSubAddress() {
    final Pattern unitWithSuffixOrPrefixPattern = Pattern
      .compile("(?:\\d+[A-Z]|[A-Z]\\d+|LC\\d+|L \\d+|A-\\d)");
    final Pattern numberRangePattern = Pattern.compile("(\\d+) ?- ?(\\d+)");

    final Set<String> ignoreUnitDescriptors = Sets.newHash("DOCK");
    final Set<String> ignoreUnitNumberSuffixes = Sets.newHash("MH", "BH");
    final Path file = this.inputDirectory.resolve("ABC_SUB_ADDRESS.csv");
    try (
      Reader<Record> reader = RecordReader.newRecordReader(file)) {
      for (final Record record : reader) {
        final String civicId = record.getValue(AddressBc.CIVIC_ID);
        final String unitNumber = record.getString(AddressBc.UNIT_NUMBER, "").trim();
        final String unitNumberSuffix = record.getString(AddressBc.UNIT_NUMBER_SUFFIX, "").trim();
        String unitDescriptor;
        if (ignoreUnitNumberSuffixes.contains(unitNumberSuffix) || unitNumberSuffix.length() == 0) {
          unitDescriptor = unitNumber;
        } else if (unitNumber.length() == 0) {
          unitDescriptor = unitNumberSuffix;
        } else {
          unitDescriptor = unitNumber + unitNumberSuffix;
        }

        if (Property.hasValue(unitDescriptor) && !ignoreUnitDescriptors.contains(unitDescriptor)) {
          if (unitDescriptor.startsWith("#")) {
            unitDescriptor = unitDescriptor.substring(1);
          }
          if (unitDescriptor.startsWith("-")) {
            unitDescriptor = unitDescriptor.substring(1);
          }
          if (unitDescriptor.endsWith("-")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 1);
          }
          if (unitDescriptor.endsWith("?")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 1);
          }
          if (unitDescriptor.endsWith("-FLR")) {
            unitDescriptor = unitDescriptor.substring(0, unitDescriptor.length() - 4);
          }
          if (unitDescriptor.equals("41BH")) {
            unitDescriptor = "41";
          }
          if (unitDescriptor.startsWith("BUILDING ")) {
            unitDescriptor = unitDescriptor.substring(9);
          }

          RangeSet range = this.unitDescriptorsById.get(civicId);
          if (range == null) {
            range = new RangeSet();
            this.unitDescriptorsById.put(civicId, range);
          }
          if (unitDescriptor.endsWith(" - 3994")) {
            range.add(Integer.parseInt(unitDescriptor.substring(0, unitDescriptor.length() - 7)));
          } else if ("17MN".equals(unitDescriptor)) {
            range.add(17);
          } else if ("6TO".equals(unitDescriptor)) {
            range.add(6);
          } else if ("14QR".equals(unitDescriptor)) {
            range.add(14);
          } else if ("1TO".equals(unitDescriptor)) {
            range.add(1);
          } else if ("19KL".equals(unitDescriptor)) {
            range.add(19);
          } else if ("ABC".equals(unitDescriptor)) {
            range.addRange('A', 'C');
          } else if ("A/B".equals(unitDescriptor)) {
            range.addRange('A', 'B');
          } else if ("A&B".equals(unitDescriptor)) {
            range.addRange('A', 'B');
          } else {
            try {
              final int number = Integer.parseInt(unitDescriptor);
              range.add(number);
            } catch (final Exception e) {
              if (unitDescriptor.length() == 1) {
                final char character = unitDescriptor.charAt(0);
                range.add(character);
              } else {
                final Matcher rangeMatcher = numberRangePattern.matcher(unitDescriptor);
                if (rangeMatcher.matches()) {
                  final int from = Integer.parseInt(rangeMatcher.group(1));
                  final int to = Integer.parseInt(rangeMatcher.group(2));
                  range.addRange(from, to);
                } else {
                  final Matcher matcher = unitWithSuffixOrPrefixPattern.matcher(unitDescriptor);
                  if (matcher.matches()) {
                    range.add(unitDescriptor.replaceAll("[ \\-]", ""));
                  } else {
                    range.add(unitDescriptor);
                    writeExtraDataWarning("ABC_SUB_ADDRESS.csv", civicId, AddressBc.UNIT_NUMBER,
                      unitDescriptor);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public void run() {
    AddressBcImportSites.deleteTempFiles(this.inputByProviderDirectory);
    this.importSites.addLabelCountNameColumns(SPLIT, "Write");
    SwingUtilities.invokeLater(() -> {
      this.importSites.getLabelCountTableModel(SPLIT)
        .getTable()
        .setSortOrder(0, SortOrder.ASCENDING);

      this.importSites.setSelectedTab(SPLIT);
    });
    loadExtendedAddress();
    loadSubAddress();

    splitRecordsByProvider();

    if (this.extraDataWriter != null) {
      this.extraDataWriter.close();
    }

    System.out.println("Unused Postal Code Count\t" + this.postalCodeById.size());

  }

  private void setUnitNumber(final Record sourceRecord) {
    final String civicId = sourceRecord.getString(AddressBc.CIVIC_ID);
    final RangeSet range = this.unitDescriptorsById.get(civicId);
    if (range != null) {
      sourceRecord.setValue(UNIT_DESCRIPTOR, range.toString());
    }
  }

  private void splitRecordsByProvider() {
    try {
      final Map<String, Object> readerProperties = new HashMap<>();
      readerProperties.put("pointXFieldName", "X_COORD");
      readerProperties.put("pointYFieldName", "Y_COORD");
      readerProperties.put("geometryFactory", Gba.GEOMETRY_FACTORY_2D);
      final Path sourceFile = this.inputDirectory.resolve("ABC_CIVIC_ADDRESS.csv");
      try (
        RecordReader sourceReader = RecordReader.newRecordReader(sourceFile)) {
        sourceReader.setProperties(readerProperties);

        final RecordDefinitionImpl recordDefinitionImpl = (RecordDefinitionImpl)sourceReader
          .getRecordDefinition();
        recordDefinitionImpl.addField(UNIT_DESCRIPTOR, DataTypes.STRING);
        recordDefinitionImpl.addField(AddressBc.BUILDING_NAME, DataTypes.STRING);
        recordDefinitionImpl.addField(AddressBc.BUILDING_TYPE, DataTypes.STRING);
        recordDefinitionImpl.addField(AddressBc.POSTAL_CODE, DataTypes.STRING);
        this.recordDefinition = recordDefinitionImpl;
        loadConfig();
        if (!isCancelled()) {

          for (final Record sourceRecord : cancellable(sourceReader)) {

            setUnitNumber(sourceRecord);

            final Object civicId = sourceRecord.get(AddressBc.CIVIC_ID);
            final String buildingName = this.buildingNameById.get(civicId);
            if (buildingName != null) {
              sourceRecord.put(AddressBc.BUILDING_NAME, buildingName);
            }
            final String buildingType = this.buildingTypeById.get(civicId);
            if (buildingType != null) {
              sourceRecord.put(AddressBc.BUILDING_TYPE, buildingType);
            }
            final String postalCode = this.postalCodeById.remove(civicId);
            if (postalCode != null) {
              sourceRecord.put(AddressBc.POSTAL_CODE, postalCode);
            }
            writeRecord(sourceRecord);
          }
        }
      }
    } catch (final Throwable e) {
      Logs.error(this, e);
    } finally {
      for (final ProviderWriter writer : this.writers) {
        writer.close();
      }
      Paths.deleteFiles(this.inputByProviderDirectory, "*.prj");
    }
  }

  private synchronized void writeExtraDataWarning(final String fileName, final String civicId,
    final String fieldName, final String fieldValue) {
    if (this.extraDataWriter == null) {
      this.extraDataWriter = Tsv
        .plainWriter(this.importSites.getDirectory().resolve("EXTENDED_FIELD_WARNING.tsv"));
      this.extraDataWriter.write("FILE_NAME", AddressBc.CIVIC_ID, "FIELD_NAME", "FIELD_VALUE");
    }
    this.extraDataWriter.write(fileName, civicId, fieldName, fieldValue);
  }

  private void writeRecord(final Record record) {
    String issuingAgency = record.getString(AddressBc.ISSUING_AGENCY);
    if (issuingAgency == null) {
      issuingAgency = record.getString(AddressBc.LOCALITY);
      if (issuingAgency == null) {
        issuingAgency = "Unknown";
      }
    }
    issuingAgency = ADDRESS_BC_PROVIDER_ALIAS.getOrDefault(issuingAgency, issuingAgency);
    ProviderWriter writer = this.writerByProvider.get(issuingAgency);
    if (writer == null) {
      final String issuingAgencyUpperCase = issuingAgency.toUpperCase();
      writer = this.writerByProvider.get(issuingAgencyUpperCase);
      if (writer == null) {
        final String dataProviderWords = CaseConverter.toCapitalizedWords(issuingAgency);
        final PartnerOrganization partnerOrganization = PartnerOrganizations
          .newPartnerOrganization(dataProviderWords);
        writer = new ProviderWriter(partnerOrganization, record);
        this.writerByProvider.put(issuingAgency, writer);
        this.writerByProvider.put(issuingAgencyUpperCase, writer);
        this.writers.add(writer);
      } else {
        this.writerByProvider.put(issuingAgency, writer);
      }
    }
    writer.writeRecord(record);

  }
}
