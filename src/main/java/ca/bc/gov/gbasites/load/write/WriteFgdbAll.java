package ca.bc.gov.gbasites.load.write;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gbasites.load.ImportSites;
import ca.bc.gov.gbasites.load.common.ProviderSitePointConverter;
import ca.bc.gov.gbasites.load.provider.addressbc.AddressBC;
import ca.bc.gov.gbasites.load.provider.geobc.GeoBC;

import com.revolsys.collection.map.CollectionMap;
import com.revolsys.collection.map.Maps;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.gis.esri.gdb.file.FileGdbWriter;
import com.revolsys.io.BaseCloseable;
import com.revolsys.io.file.AtomicPathUpdator;
import com.revolsys.parallel.process.ProcessNetwork;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinitionImpl;
import com.revolsys.util.Cancellable;
import com.revolsys.util.Counter;

public class WriteFgdbAll implements Cancellable, Runnable {

  private final ImportSites dialog;

  private final List<AtomicPathUpdator> pathUpdators = new ArrayList<>();

  private final List<FileGdbRecordStore> recordStores = new ArrayList<>();

  private final List<FileGdbWriter> writers = new ArrayList<>();

  private final CollectionMap<String, Record, List<Record>> emergencyManagementSitesByLocality;

  public WriteFgdbAll(final ImportSites dialog,
    final CollectionMap<String, Record, List<Record>> emergencyManagementSitesByLocality) {
    this.dialog = dialog;
    this.emergencyManagementSitesByLocality = emergencyManagementSitesByLocality;
  }

  private void close(final List<? extends BaseCloseable> closeables, final int index) {
    final BaseCloseable closeable = closeables.get(index);
    try {
      closeable.close();
    } catch (final Exception e) {
      Logs.error(this, "Error closing: " + closeable, e);
    }
  }

  @Override
  public boolean isCancelled() {
    return this.dialog.isCancelled();
  }

  private FileGdbWriter newWriter(final String fileSuffix) {
    final RecordDefinitionImpl recordDefinition = ImportSites.getSitePointFgdbRecordDefinition();
    final String fileName = "SITE_POINT" + fileSuffix + ".gdb";
    final Path directory = ImportSites.SITES_DIRECTORY.resolve("FGDB");
    final AtomicPathUpdator pathUpdator = ImportSites.newPathUpdator(this.dialog, directory,
      fileName);
    this.pathUpdators.add(pathUpdator);

    final Path path = pathUpdator.getPath();
    final FileGdbRecordStore recordStore = FileGdbRecordStoreFactory
      .newRecordStoreInitialized(path);
    this.recordStores.add(recordStore);

    final FileGdbWriter writer = recordStore.newRecordWriter(recordDefinition);
    this.writers.add(writer);

    return writer;
  }

  @Override
  public void run() {
    try {
      final FileGdbWriter allWriter = newWriter("_ALL");
      final FileGdbWriter emWriter = newWriter("_EM");
      final Map<Path, FileGdbWriter> writerByDirectory = Maps
        .<Path, FileGdbWriter> buildLinkedHash() //
        .add(ProviderSitePointConverter.PROVIDER_DIRECTORY, newWriter("_PROVIDER")) //
        .add(GeoBC.DIRECTORY, newWriter("_GEOBC")) //
        .add(AddressBC.DIRECTORY, newWriter("_ADDRESSBC")) //
        .getMap()//
      ;

      final Collection<String> localityNames = GbaController.getLocalities().getBoundaryNames();
      for (final String localityName : cancellable(localityNames)) {
        final Counter allCounter = this.dialog.getCounter("FGDB", localityName, "All");

        // emergency management records
        final List<Record> emRecords = this.emergencyManagementSitesByLocality
          .getOrEmpty(localityName);
        if (!emRecords.isEmpty()) {
          final Counter emCounter = this.dialog.getCounter("FGDB", localityName, "EM");
          for (final Record record : emRecords) {
            emWriter.writeNewRecord(record);
            emCounter.add();

            allWriter.writeNewRecord(record);
            allCounter.add();
          }
        }

        // Records for each provider
        for (final Entry<Path, FileGdbWriter> entry : writerByDirectory.entrySet()) {
          final Path directory = entry.getKey();
          final FileGdbWriter providerWriter = entry.getValue();
          final Counter providerCounter = this.dialog.getCounter("FGDB", localityName,
            directory.getFileName().toString());

          final List<Path> providerFiles = ImportSites.SITE_POINT_BY_LOCALITY
            .listLocalityFiles(directory, localityName);

          for (final Path localityFile : providerFiles) {
            try (
              RecordReader reader = RecordReader.newRecordReader(localityFile)) {
              for (final Record record : cancellable(reader)) {
                providerWriter.writeNewRecord(record);
                providerCounter.add();
                allWriter.writeNewRecord(record);
                allCounter.add();
              }
            }
          }
        }
      }
    } finally {
      final ProcessNetwork closeProcesses = new ProcessNetwork();

      for (int i = 0; i < this.writers.size(); i++) {
        final int index = i;
        closeProcesses.addProcess("FGDB-Close-" + i, () -> {
          close(this.writers, index);
          close(this.recordStores, index);
          close(this.pathUpdators, index);
        });
      }
      closeProcesses.startAndWait();
    }
  }

}
