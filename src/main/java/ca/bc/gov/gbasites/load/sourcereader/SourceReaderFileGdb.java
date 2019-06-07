package ca.bc.gov.gbasites.load.sourcereader;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import org.jeometry.common.io.PathName;

import com.revolsys.collection.map.MapEx;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStore;
import com.revolsys.gis.esri.gdb.file.FileGdbRecordStoreFactory;
import com.revolsys.record.io.RecordReader;

public class SourceReaderFileGdb extends AbstractRecordReaderSourceReader {

  public static Function<MapEx, SourceReaderFileGdb> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SourceReaderFileGdb(properties.addAll(config));
  }

  private FileGdbRecordStore recordStore;

  public SourceReaderFileGdb(final MapEx properties) {
    super(properties);
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      if (this.recordStore != null) {
        this.recordStore.close();
      }
    }
  }

  @Override
  protected RecordReader newRecordReader() {
    final String gdbFileName = getProperty("gdbFileName");
    final String typePath = getProperty("typePath");
    final Path sourceFile = this.baseDirectory.getParent()
      .resolve("Input")
      .resolve(this.dataProvider)
      .resolve(gdbFileName);

    try (
      FileGdbRecordStore recordStore = FileGdbRecordStoreFactory.newRecordStore(sourceFile)) {
      recordStore.setCreateMissingRecordStore(false);
      recordStore.setCreateMissingTables(false);
      recordStore.initialize();
      this.recordStore = recordStore;
      return recordStore.getRecords(PathName.newPathName(typePath));
    }
  }
}
