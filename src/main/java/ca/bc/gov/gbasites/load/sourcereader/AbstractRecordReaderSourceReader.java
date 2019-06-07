package ca.bc.gov.gbasites.load.sourcereader;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.schema.RecordDefinition;

public abstract class AbstractRecordReaderSourceReader extends AbstractSourceReader {

  private RecordReader reader;

  public AbstractRecordReaderSourceReader(final MapEx properties) {
    super(properties);
  }

  @Override
  public void close() {
    super.close();
    if (this.reader != null) {
      this.reader.close();
    }
  }

  @Override
  protected RecordDefinition getSourceRecordDefinitionDo() {
    this.reader = newRecordReader();
    if (this.reader == null) {
      return null;
    } else {
      return this.reader.getRecordDefinition();
    }
  }

  protected abstract RecordReader newRecordReader();

  @Override
  protected void writeRecords() {
    writeRecords(this.reader);
  }

}
