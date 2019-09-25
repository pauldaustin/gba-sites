package ca.bc.gov.gbasites.export;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataTypes;
import org.jeometry.common.io.PathName;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.Gba;
import ca.bc.gov.gba.model.GbaTables;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gba.ui.StatisticsDialog;
import ca.bc.gov.gbasites.model.type.SitePoint;
import ca.bc.gov.gbasites.model.type.SiteTables;

import com.revolsys.geometry.model.GeometryDataTypes;
import com.revolsys.gis.esri.gdb.file.FileGdbWriterProcess;
import com.revolsys.parallel.channel.Channel;
import com.revolsys.parallel.process.AbstractOutProcess;
import com.revolsys.parallel.process.ConsumerOutProcess;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.code.SingleValueCodeTable;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Cancellable;
import com.revolsys.util.count.LabelCounters;

public class SitePointExport implements SitePoint, Cancellable {

  private static final String SITE_TYPE = "SITE_TYPE";

  private static final String SITE_LOCATION = "SITE_LOCATION";

  private static final String CUSTODIAN_INTEGRATION_DATE = "CUSTODIAN_INTEGRATION_DATE";

  private static final String MODIFY_INTEGRATION_DATE = "MODIFY_INTEGRATION_DATE";

  private static final String CREATE_INTEGRATION_DATE = "CREATE_INTEGRATION_DATE";

  private static final String CUSTODIAN_PARTNER_ORG = "CUSTODIAN_PARTNER_ORG";

  private static final String MODIFY_PARTNER_ORG = "MODIFY_PARTNER_ORG";

  private static final String CREATE_PARTNER_ORG = "CREATE_PARTNER_ORG";

  private static final String FEATURE_STATUS = "FEATURE_STATUS";

  private static final String STREET_NAME_ALIAS_1 = "STREET_NAME_ALIAS_1";

  private static final String COMMUNITY = "COMMUNITY";

  private static final String REGIONAL_DISTRICT = "REGIONAL_DISTRICT";

  private static final String LOCALITY = "LOCALITY";

  private static final String DATA_CAPTURE_METHOD = "DATA_CAPTURE_METHOD";

  public static void main(final String[] args) {
    final SitePointExport process = new SitePointExport();
    BatchUpdateDialog.start(process::batchUpdate, "Site Point Export", BatchUpdateDialog.READ,
      BatchUpdateDialog.WRITE);
  }

  private BatchUpdateDialog dialog;

  private final RecordStore recordStore = GbaController.getRecordStore();

  private RecordDefinition sitePointExportRecordDefinition;

  public SitePointExport() {
  }

  protected boolean batchUpdate(final BatchUpdateDialog dialog, final Transaction transaction) {
    this.dialog = dialog;
    ProcessNetwork.startAndWait(() -> {

      final AbstractOutProcess<Record> readProcess = new ConsumerOutProcess<>(this::readRecords) //
        .setOutBufferSize(100);

      final Path sitePointFile = GbaController.getDataPath("exports/site_point.gdb");

      final FileGdbWriterProcess writerProcess = new FileGdbWriterProcess(sitePointFile);
      newSitePointExportRecordDefinition(writerProcess);

      final LabelCounters writeCounts = this.dialog.getLabelCountMap(StatisticsDialog.COUNTS,
        BatchUpdateDialog.WRITE);
      writerProcess //
        .setCounts(writeCounts)//
        .setCancellable(this) //
        .setIn(readProcess) //
      ;
    });
    return true;
  }

  private CodeTable getIntegrationSessionCodeTable() {
    final SingleValueCodeTable integrationSessionCodeTable = new SingleValueCodeTable(
      "INTEGRATION_SESSION_DATE");
    final Query query = new Query(GbaTables.INTEGRATION_SESSION_POLY) //
      .setFieldNames(Gba.INTEGRATION_SESSION_POLY_ID, Gba.SESSION_COMMIT_DATE);
    for (final Record record : this.recordStore.getRecords(query)) {
      final Identifier id = record.getIdentifier(Gba.INTEGRATION_SESSION_POLY_ID);
      final Date date = record.getValue(Gba.SESSION_COMMIT_DATE);
      integrationSessionCodeTable.addValue(id, date);
    }
    return integrationSessionCodeTable;
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  private Query newQuery(final PathName pathName) {
    final Query query;
    final RecordDefinition recordDefinition = this.recordStore.getRecordDefinition(pathName);
    if (recordDefinition == null) {
      query = null;
    } else {
      final String idFieldName = recordDefinition.getIdFieldName();
      query = Query.orderBy(pathName, idFieldName);
      final LabelCounters counts = this.dialog.getLabelCountMap(StatisticsDialog.COUNTS,
        BatchUpdateDialog.READ);
      query.setStatistics(counts);
      this.dialog.newLabelCount(StatisticsDialog.COUNTS, pathName, BatchUpdateDialog.READ);
    }
    return query;
  }

  private void newSitePointExportRecordDefinition(final FileGdbWriterProcess writerProcess) {
    final RecordDefinition recordDefinition = this.recordStore
      .getRecordDefinition(SiteTables.SITE_POINT);

    final CodeTable integrationSessionCodeTable = getIntegrationSessionCodeTable();

    final RecordDefinitionImpl exportRecordDefinition = new RecordDefinitionImpl(
      SiteTables.SITE_POINT);
    final List<FieldDefinition> idAndCodeFields = new ArrayList<>();
    for (final FieldDefinition sourceFieldDefinition : recordDefinition.getFields()) {
      final FieldDefinition exportFieldDefinition = sourceFieldDefinition.clone();
      final String fieldName = exportFieldDefinition.getName();
      boolean idOrCode = true;
      boolean ignoreField = false;
      if (LOCALITY_ID.equals(fieldName)) {
        exportRecordDefinition.addField(LOCALITY, DataTypes.STRING, 50, true);
      } else if (REGIONAL_DISTRICT_ID.equals(fieldName)) {
        exportRecordDefinition.addField(REGIONAL_DISTRICT, DataTypes.STRING, 50, true);
      } else if (COMMUNITY_ID.equals(fieldName)) {
        exportRecordDefinition.addField(COMMUNITY, DataTypes.STRING, 50, false);
      } else if (STREET_NAME_ALIAS_1_ID.equals(fieldName)) {
        exportRecordDefinition.addField(STREET_NAME_ALIAS_1, DataTypes.STRING, 100, false);
      } else if (FEATURE_STATUS_CODE.equals(fieldName)) {
        exportRecordDefinition.addField(FEATURE_STATUS, DataTypes.STRING, 30, true);
      } else if (CREATE_PARTNER_ORG_ID.equals(fieldName)) {
        exportRecordDefinition.addField(CREATE_PARTNER_ORG, DataTypes.STRING, 50, true);
      } else if (MODIFY_PARTNER_ORG_ID.equals(fieldName)) {
        exportRecordDefinition.addField(MODIFY_PARTNER_ORG, DataTypes.STRING, 50, true);
      } else if (CUSTODIAN_PARTNER_ORG_ID.equals(fieldName)) {
        exportRecordDefinition.addField(CUSTODIAN_PARTNER_ORG, DataTypes.STRING, 50, true);
      } else if (CREATE_INTEGRATION_SESSION_ID.equals(fieldName)) {
        sourceFieldDefinition.setCodeTable(integrationSessionCodeTable);
        exportRecordDefinition.addField(CREATE_INTEGRATION_DATE, DataTypes.DATE, true);
      } else if (MODIFY_INTEGRATION_SESSION_ID.equals(fieldName)) {
        sourceFieldDefinition.setCodeTable(integrationSessionCodeTable);
        exportRecordDefinition.addField(MODIFY_INTEGRATION_DATE, DataTypes.DATE, true);
      } else if (CUSTODIAN_SESSION_ID.equals(fieldName)) {
        sourceFieldDefinition.setCodeTable(integrationSessionCodeTable);
        exportRecordDefinition.addField(CUSTODIAN_INTEGRATION_DATE, DataTypes.DATE, false);
      } else if (DATA_CAPTURE_METHOD_CODE.equals(fieldName)) {
        exportRecordDefinition.addField(DATA_CAPTURE_METHOD, DataTypes.STRING, 30, false);
      } else if (SITE_LOCATION_CODE.equals(fieldName)) {
        exportRecordDefinition.addField(SITE_LOCATION, DataTypes.STRING, 30, false);
      } else if (SITE_TYPE_CODE.equals(fieldName)) {
        exportRecordDefinition.addField(SITE_TYPE, DataTypes.STRING, 30, false);
      } else if (GEOMETRY.equals(fieldName)) {
        ignoreField = true;
      } else {
        idOrCode = false;
      }
      if (!ignoreField) {
        if (idOrCode) {
          idAndCodeFields.add(exportFieldDefinition);
        } else {
          exportRecordDefinition.addField(exportFieldDefinition);
        }
      }
    }
    for (final FieldDefinition fieldDefinition : idAndCodeFields) {
      exportRecordDefinition.addField(fieldDefinition);
    }
    exportRecordDefinition.addField("POINT", GeometryDataTypes.POINT, true);
    exportRecordDefinition.setGeometryFactory(recordDefinition.getGeometryFactory());

    final RecordStore exportRecordStore = writerProcess.getRecordStore();
    this.sitePointExportRecordDefinition = exportRecordStore
      .getRecordDefinition(exportRecordDefinition);
  }

  private void readRecords(final Channel<Record> out) {
    final List<Query> queries = new ArrayList<>();
    for (final PathName pathName : Arrays.asList( //
      GbaTables.DATA_CAPTURE_METHOD_CODE, //
      SiteTables.FEATURE_STATUS_CODE, //
      GbaTables.PARTNER_ORGANIZATION, //
      SiteTables.SITE_LOCATION_CODE, //
      SiteTables.SITE_TYPE_CODE)) {
      final Query query = newQuery(pathName);
      queries.add(query);
    }
    try (
      RecordReader codeReader = this.recordStore.getRecords(queries)) {
      for (final Record record : cancellable(codeReader)) {
        out.write(record);
      }
    }
    final Query sitePointQuery = newQuery(SiteTables.SITE_POINT);
    try (
      RecordReader sitePointReader = this.recordStore.getRecords(sitePointQuery)) {
      final RecordDefinition sitePointExportRecordDefinition = this.sitePointExportRecordDefinition;
      for (final Record record : cancellable(sitePointReader)) {
        final Record exportRecord = sitePointExportRecordDefinition.newRecord(record);
        exportRecord.setGeometryValue(record);
        exportRecord.setCodeValue(LOCALITY, record, LOCALITY_ID);
        exportRecord.setCodeValue(REGIONAL_DISTRICT, record, REGIONAL_DISTRICT_ID);
        exportRecord.setCodeValue(COMMUNITY, record, COMMUNITY_ID);
        exportRecord.setCodeValue(STREET_NAME_ALIAS_1, record, STREET_NAME_ALIAS_1_ID);
        exportRecord.setCodeValue(FEATURE_STATUS, record, FEATURE_STATUS_CODE);
        exportRecord.setCodeValue(CREATE_PARTNER_ORG, record, CREATE_PARTNER_ORG_ID);
        exportRecord.setCodeValue(MODIFY_PARTNER_ORG, record, MODIFY_PARTNER_ORG_ID);
        exportRecord.setCodeValue(CUSTODIAN_PARTNER_ORG, record, CUSTODIAN_PARTNER_ORG_ID);

        exportRecord.setCodeValue(CREATE_INTEGRATION_DATE, record, CREATE_INTEGRATION_SESSION_ID);
        exportRecord.setCodeValue(MODIFY_INTEGRATION_DATE, record, MODIFY_INTEGRATION_SESSION_ID);
        exportRecord.setCodeValue(CUSTODIAN_INTEGRATION_DATE, record, CUSTODIAN_SESSION_ID);
        exportRecord.setCodeValue(DATA_CAPTURE_METHOD, record, DATA_CAPTURE_METHOD_CODE);
        exportRecord.setCodeValue(SITE_TYPE, record, SITE_TYPE_CODE);
        exportRecord.setCodeValue(SITE_LOCATION, record, SITE_LOCATION_CODE);
        out.write(exportRecord);
      }
    }
  }
}
