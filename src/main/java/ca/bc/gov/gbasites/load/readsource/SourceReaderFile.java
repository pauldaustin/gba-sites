package ca.bc.gov.gbasites.load.readsource;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.jeometry.common.logging.Logs;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordReaderFactory;
import com.revolsys.util.Property;
import com.revolsys.util.SupplierWithProperties;
import com.revolsys.util.UrlUtil;

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

  private Path getLocalSourceFilePath(final String fileName) {
    return this.baseDirectory//
      .resolve("Source")
      .resolve(getPartnerOrganizationShortName())
      .resolve(fileName);
  }

  @Override
  protected RecordReader newRecordReader() {
    String fileName = this.fileName;
    if (Property.hasValue(fileName)) {
      final Path source = getLocalSourceFilePath(fileName);
      final MapEx readerProperties = getProperty("readerProperties");
      return RecordReader.newRecordReader(source, readerProperties);
    } else {
      if (this.fileReaderFactory == null) {
        throw new IllegalArgumentException(
          "Config must have fileName, fileUrl, of fileReaderFactory:" + getProperties());
      } else {
        try {
          return this.fileReaderFactory.get();
        } catch (final RuntimeException e) {
          if (this.fileReaderFactory instanceof SupplierWithProperties<?>) {
            final SupplierWithProperties<?> supplier = (SupplierWithProperties<?>)this.fileReaderFactory;

            final String fileUrl = supplier.getProperty("fileUrl");
            fileName = UrlUtil.getFileName(fileUrl);
            final Path source = getLocalSourceFilePath(fileName);
            try {
              final MapEx readerProperties = new LinkedHashMapEx();
              readerProperties.putAll(supplier.getProperties());
              readerProperties.put("fileName", source.toString());
              return RecordReaderFactory.newRecordReaderSupplier(readerProperties).get();
            } catch (final RuntimeException e1) {
              throw e;
            } finally {
              Logs.warn(this, "Using local file, cannot download\n  " + source + "\n  " + fileUrl);
            }
          } else {
            throw e;
          }
        }
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
