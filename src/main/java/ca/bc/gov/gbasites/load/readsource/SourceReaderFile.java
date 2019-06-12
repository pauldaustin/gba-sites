package ca.bc.gov.gbasites.load.readsource;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.io.RecordReader;
import com.revolsys.util.Property;

public class SourceReaderFile extends AbstractRecordReaderSourceReader {

  public static Function<MapEx, SourceReaderFile> newFactory(
    final Map<String, ? extends Object> config) {
    return properties -> new SourceReaderFile(properties.addAll(config));
  }

  private String fileName;

  private Supplier<RecordReader> fileReaderFactory;

  public SourceReaderFile(final MapEx properties) {
    super(properties);
  }

  @Override
  protected RecordReader newRecordReader() {
    if (Property.hasValue(this.fileName)) {
      final Object source = this.baseDirectory//
        .resolve("Source")
        .resolve(getPartnerOrganizationShortName())
        .resolve(this.fileName);
      final RecordReader reader = RecordReader.newRecordReader(source);
      final Map<String, Object> readerProperties = getProperty("readerProperties");
      reader.setProperties(readerProperties);
      return reader;
    } else {
      if (this.fileReaderFactory == null) {
        throw new IllegalArgumentException(
          "Config must have fileName, fileUrl, of fileReaderFactory:" + getProperties());
      } else {
        return this.fileReaderFactory.get();
      }
    }
  }

  public void setFileName(final String fileName) {
    this.fileName = fileName;
  }

  public void setFileReaderFactory(final Supplier<RecordReader> fileReaderFactory) {
    this.fileReaderFactory = fileReaderFactory;
  }
}
