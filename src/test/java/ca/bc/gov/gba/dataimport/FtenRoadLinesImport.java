package ca.bc.gov.gba.dataimport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.data.type.DataType;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaConfig;
import ca.bc.gov.gba.core.model.CountNames;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;

import com.revolsys.geometry.model.Geometry;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.Polygonal;
import com.revolsys.io.file.Paths;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.RecordState;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transaction;

public class FtenRoadLinesImport {
  private static final PathName FTEN_ROAD_LINES = PathName
    .newPathName("/WHSE_FOREST_TENURE/FTEN_ROAD_LINES");

  private static final String BACKUP_READ = "Backup Read";

  public static void main(final String[] args) {
    final FtenRoadLinesImport process = new FtenRoadLinesImport();
    BatchUpdateDialog.start(process::batchUpdate, FtenRoadLinesImport.class.getName(), BACKUP_READ,
      CountNames.READ, CountNames.IGNORED, CountNames.INSERTED, CountNames.UPDATED,
      CountNames.DELETED);
  }

  private final Path backupDirectory = GbaConfig.getDataDirectory()
    .resolve("warehouse/WHSE_FOREST_TENURE/data");

  private final Map<PathName, List<Identifier>> deletedIdentifiersByTypePath = new LinkedHashMap<>();

  private BatchUpdateDialog dialog;

  private final RecordStore gbaRecordStore = GbaSiteDatabase.getRecordStore();

  protected boolean batchUpdate(final BatchUpdateDialog dialog, final Transaction transaction) {
    this.dialog = dialog;
    final ProcessNetwork processNetwork = new ProcessNetwork();
    processNetwork.addProcess(() -> {
      mergeRecordsForType(FTEN_ROAD_LINES, null);
    });
    processNetwork.startAndWait();
    deleteRecords(FTEN_ROAD_LINES);
    return true;
  }

  private void deleteRecord(final PathName typePath, final Identifier identifier) {
    try (
      Transaction transaction = this.gbaRecordStore.newTransaction()) {
      this.gbaRecordStore.deleteRecord(typePath, identifier);
      final BatchUpdateDialog r = this.dialog;
      r.addLabelCount(CountNames.COUNTS, typePath, CountNames.DELETED);
    }
  }

  private void deleteRecords(final PathName typePath) {
    final List<Identifier> identifiers = this.deletedIdentifiersByTypePath.get(typePath);
    if (identifiers != null) {
      for (final Identifier identifier : identifiers) {
        deleteRecord(typePath, identifier);
      }
    }
  }

  public Record getBackupRecord(final Iterator<Record> backupIterator) {
    while (backupIterator.hasNext()) {
      final Record record = backupIterator.next();
      return record;
    }
    return null;
  }

  private void insertRecord(final PathName typePath, final Record backupRecord) {
    try (
      Transaction transaction = this.gbaRecordStore.newTransaction();
      RecordWriter gbaWriter = this.gbaRecordStore.newRecordWriter()) {
      final RecordDefinition recordDefinition = this.gbaRecordStore.getRecordDefinition(typePath);
      final Record gbaRecord = this.gbaRecordStore.newRecord(typePath);
      setGbaValues(recordDefinition, backupRecord, gbaRecord);
      gbaWriter.write(gbaRecord);
      final BatchUpdateDialog r = this.dialog;
      r.addLabelCount(CountNames.COUNTS, typePath, CountNames.INSERTED);
    }
  }

  private void mergeRecordsForType(final PathName typePath, final Predicate<Record> filter) {
    final RecordDefinition recordDefinition = this.gbaRecordStore.getRecordDefinition(typePath);
    if (recordDefinition == null) {
      Logs.error(this, typePath + " does not exist");
    } else {
      final String idFieldName = recordDefinition.getIdFieldName();

      final Query query = new Query(typePath);
      query.addOrderBy(idFieldName, true);
      final Path backupFilePath = Paths.getPath(this.backupDirectory, typePath.getName() + ".tsv");

      final List<Identifier> deletedIdentifiers = new ArrayList<>();
      final List<Record> failedInsertRecords = new ArrayList<>();
      final List<Record> failedUpdateRecords = new ArrayList<>();
      try (
        RecordReader backupReader = RecordReader.newRecordReader(backupFilePath);
        RecordReader gbaReader = this.gbaRecordStore.getRecords(query);) {

        final Iterator<Record> backupIterator = this.dialog.cancellable(backupReader.iterator());
        final Iterator<Record> gbaIterator = this.dialog.cancellable(gbaReader.iterator());

        Record backupRecord = null;
        Record gbaRecord = null;

        Identifier backupIdentifier = null;
        Identifier gbaIdentifier = null;

        Identifier maxId;
        try {
          maxId = this.gbaRecordStore.newPrimaryIdentifier(typePath);
        } catch (final Throwable e) {
          maxId = null;
        }
        try (
          Transaction transaction = this.gbaRecordStore.newTransaction()) {
          while (gbaIterator.hasNext() && backupIterator.hasNext()) {
            if (backupRecord == null) {
              this.dialog.addLabelCount(CountNames.COUNTS, typePath, BACKUP_READ);
              backupRecord = getBackupRecord(backupIterator);
              backupIdentifier = backupRecord.getIdentifier(idFieldName);
              if (maxId != null) {
                while (maxId.compareTo(backupIdentifier) < 0) {
                  maxId = this.gbaRecordStore.newPrimaryIdentifier(typePath);
                }
              }
            }
            if (gbaRecord == null) {
              this.dialog.addLabelCount(CountNames.COUNTS, typePath, CountNames.READ);
              gbaRecord = gbaIterator.next();
              gbaIdentifier = gbaRecord.getIdentifier(idFieldName);
            }

            final int idCompare = gbaIdentifier.compareTo(backupIdentifier);
            if (idCompare > 0) {
              if (filter == null || filter.test(backupRecord)) {
                try {
                  insertRecord(typePath, backupRecord);
                } catch (final Throwable t) {
                  failedInsertRecords.add(backupRecord);
                }
              } else {
                this.dialog.addLabelCount(CountNames.COUNTS, typePath, CountNames.IGNORED);
              }
              backupRecord = null;
            } else if (idCompare < 0) {
              try {
                deleteRecord(typePath, gbaIdentifier);
              } catch (final Throwable t) {
                deletedIdentifiers.add(gbaIdentifier);
              }
              gbaRecord = null;
            } else {
              if (filter == null || filter.test(backupRecord)) {
                setGbaValues(recordDefinition, backupRecord, gbaRecord);
                if (gbaRecord.getState() == RecordState.MODIFIED) {
                  try {
                    updateRecord(gbaRecord);
                  } catch (final Throwable t) {
                    failedUpdateRecords.add(gbaRecord);
                  }
                }
              } else {
                try {
                  deleteRecord(typePath, backupIdentifier);
                } catch (final Throwable t) {
                  failedUpdateRecords.add(gbaRecord);
                }
              }
              backupRecord = null;
              gbaRecord = null;
            }
          }
          while (backupIterator.hasNext()) {
            this.dialog.addLabelCount(CountNames.COUNTS, typePath, BACKUP_READ);
            backupRecord = getBackupRecord(backupIterator);
            if (filter == null || filter.test(backupRecord)) {
              backupIdentifier = backupRecord.getIdentifier(idFieldName);
              if (maxId != null) {
                while (maxId.compareTo(backupIdentifier) < 0) {
                  maxId = this.gbaRecordStore.newPrimaryIdentifier(typePath);
                }
              }
              try {
                insertRecord(typePath, backupRecord);
              } catch (final Throwable t) {
                failedInsertRecords.add(backupRecord);
              }
            } else {
              this.dialog.addLabelCount(CountNames.COUNTS, typePath, CountNames.IGNORED);
            }
          }
          while (gbaIterator.hasNext()) {
            gbaRecord = gbaIterator.next();
            gbaIdentifier = gbaRecord.getIdentifier(idFieldName);
            try {
              deleteRecord(typePath, gbaIdentifier);
            } catch (final Throwable t) {
              deletedIdentifiers.add(gbaIdentifier);
            }
          }
        }
        for (final Record record : failedInsertRecords) {
          try {
            insertRecord(typePath, record);
          } catch (final Throwable e) {
            Logs.error(this, "Failed to insert:\n" + record, e);
          }
        }
        for (final Record record : failedUpdateRecords) {
          try {
            updateRecord(record);
          } catch (final Throwable e) {
            Logs.error(this, "Failed to update:\n" + record, e);
          }
        }
      }

      synchronized (this.deletedIdentifiersByTypePath) {
        this.deletedIdentifiersByTypePath.put(typePath, deletedIdentifiers);
      }
    }
  }

  protected void setGbaValues(final RecordDefinition recordDefinition, final Record backupRecord,
    final Record gbaRecord) {
    for (final FieldDefinition fieldDefinition : recordDefinition.getFields()) {
      final String name = fieldDefinition.getName();
      if ("GEOMETRY".equals(name)) {
        Geometry backupGeometry = backupRecord.getGeometry();
        if (backupGeometry != null) {
          final GeometryFactory geometryFactory = recordDefinition.getGeometryFactory();
          backupGeometry = backupGeometry.convertGeometry(geometryFactory);
          final Geometry gbaGeometry = gbaRecord.getGeometry();
          if (gbaGeometry != null) {
            if (gbaGeometry instanceof Polygonal || backupGeometry instanceof Polygonal) {
              if (gbaGeometry.equalsExactNormalize(backupGeometry)) {
                backupGeometry = gbaGeometry;
              }
            }
          }
          gbaRecord.setGeometryValue(backupGeometry);
        }
      } else {
        final DataType dataType = fieldDefinition.getDataType();
        final Object backupValue = backupRecord.getValue(name);
        final Object convertedValue = dataType.toObject(backupValue);
        gbaRecord.setValue(name, convertedValue);
      }
    }
  }

  private void updateRecord(final Record gbaRecord) {
    try (
      Transaction transaction = this.gbaRecordStore.newTransaction();
      RecordWriter gbaWriter = this.gbaRecordStore.newRecordWriter();) {
      gbaWriter.write(gbaRecord);
      final BatchUpdateDialog r = this.dialog;
      r.addLabelCount(CountNames.COUNTS, gbaRecord.getPathName(), CountNames.UPDATED);
    }
  }
}
