package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.logging.Logs;

import ca.bc.gov.gba.core.model.CountNames;
import ca.bc.gov.gba.core.model.Gba;
import ca.bc.gov.gba.core.model.codetable.BoundaryCache;
import ca.bc.gov.gba.itn.model.code.GbaItnCodeTables;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.geometry.model.Point;
import com.revolsys.io.file.Paths;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class GeocoderCaSplitByLocality implements Cancellable, SitePoint {
  private class LocalityWriter {

    private final String localityName;

    private final RecordWriter recordWriter;

    private final Path targetPath;

    private int writeCount = 0;

    private final Path writePath;

    public LocalityWriter(final Identifier localityId,
      final RecordDefinitionProxy recordDefinition) {
      this.localityName = LOCALITIES.getValue(localityId);
      final String baseName = Gba.toFileName(this.localityName) + "_GEOCODER_CA";
      final String fileName = baseName + ".tsv";
      final Path directory = GeocoderCaSplitByLocality.this.inputByLocalityDirectory;
      this.writePath = directory.resolve("_" + fileName);
      this.targetPath = directory.resolve(fileName);
      this.recordWriter = RecordWriter.newRecordWriter(recordDefinition, this.writePath);
      this.recordWriter.setProperty("useQuotes", false);
      Paths.deleteDirectories(directory.resolve("_" + baseName + ".prj"));
    }

    void close() {
      this.recordWriter.close();
      if (this.writeCount == 0) {
        try {
          Files.deleteIfExists(this.targetPath);
        } catch (final IOException e) {
          Logs.error(this, "Unable to delete: " + this.targetPath, e);
        }
        try {
          Files.deleteIfExists(this.writePath);
        } catch (final IOException e) {
          Logs.error(this, "Unable to delete: " + this.writePath, e);
        }
      } else {
        try {
          Files.move(this.writePath, this.targetPath, StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException e) {
          Logs.error(this, "Unable to rename " + this.writePath + " to " + this.targetPath, e);
        }
      }
    }

    @Override
    public String toString() {
      return this.localityName;
    }

    public void writeRecord(final Record record) {
      GeocoderCaSplitByLocality.this.counts.addCount(this.localityName, CountNames.READ);
      GeocoderCaSplitByLocality.this.counts.addCount(this.localityName, CountNames.WRITE);
      this.recordWriter.write(record);
      this.writeCount++;
    }
  }

  private static final BoundaryCache LOCALITIES = GbaItnCodeTables.getLocalities();

  private static final String SPLIT = "Split";

  private final Path inputDirectory;

  private final Path inputByLocalityDirectory;

  private final GeocoderCaImportSites importSites;

  private final Map<Identifier, LocalityWriter> writerByLocalityId = new HashMap<>();

  private final List<LocalityWriter> writers = new ArrayList<>();

  private LabelCountMapTableModel counts;

  public GeocoderCaSplitByLocality(final GeocoderCaImportSites importSites) {
    this.importSites = importSites;
    this.inputDirectory = importSites.getInputDirectory();
    this.inputByLocalityDirectory = importSites.getInputByProviderDirectory();
    Paths.createDirectories(this.inputByLocalityDirectory);
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  public void run() {
    this.counts = this.importSites.labelCounts(SPLIT, "Locality", CountNames.READ,
      CountNames.WRITE);
    SwingUtilities.invokeLater(() -> {
      this.counts.getTable().setSortOrder(0, SortOrder.ASCENDING);
      this.importSites.setSelectedTab(SPLIT);
    });

    splitRecordsByProvider();
  }

  private void splitRecordsByProvider() {
    try {
      final Path sourceFile = this.inputDirectory.resolve("GEOCODER_CA_ADDRESS_POINT.tsv");
      try (
        RecordReader sourceReader = RecordReader.newRecordReader(sourceFile)) {
        for (final Record sourceRecord : cancellable(sourceReader)) {
          writeRecord(sourceRecord);
        }
      }
    } catch (final Throwable e) {
      Logs.error(this, e);
    } finally {
      for (final LocalityWriter writer : this.writers) {
        writer.close();
      }
      Paths.deleteFiles(this.inputByLocalityDirectory, "*.prj");
    }
  }

  private void writeRecord(final Record record) {
    final Point point = record.getGeometry();
    final Identifier localityId = LOCALITIES.getBoundaryId(point);
    LocalityWriter writer = this.writerByLocalityId.get(localityId);
    if (writer == null) {
      writer = new LocalityWriter(localityId, record);
      this.writerByLocalityId.put(localityId, writer);
      this.writers.add(writer);
    }
    writer.writeRecord(record);
  }
}
