package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import javax.swing.SortOrder;
import javax.swing.SwingUtilities;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.coordinatesystem.model.systems.EpsgId;

import ca.bc.gov.gba.core.model.CountNames;
import ca.bc.gov.gba.core.model.Gba;
import ca.bc.gov.gba.core.model.codetable.BoundaryCache;
import ca.bc.gov.gba.itn.model.code.GbaItnCodeTables;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.io.RecordWriter;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionBuilder;
import com.revolsys.swing.table.counts.LabelCountMapTableModel;
import com.revolsys.util.Cancellable;

public class GeocoderCaFilterBC implements Cancellable, SitePoint {

  public static final String EXTRACT_BC = "Extract BC";

  private static final String INSIDE_BC = "INSIDE_BC";

  private static final String OUTSIDE_BC = "OUTSIDE_BC";

  private static final BoundaryCache REGIONAL_DISTRICTS = GbaItnCodeTables.getRegionalDistricts();

  private LabelCountMapTableModel counts;

  private final GeocoderCaImportSites importSites;

  private final List<Identifier> REGIONAL_DISTRICTS_OUTSIDE_BC = Arrays.asList(//
    Identifier.newIdentifier("AB"), //
    Identifier.newIdentifier("AK"), //
    Identifier.newIdentifier("ID"), //
    Identifier.newIdentifier("MO"), //
    Identifier.newIdentifier("NWT"), //
    Identifier.newIdentifier("WA"), //
    Identifier.newIdentifier("YT") //
  );

  public GeocoderCaFilterBC(final GeocoderCaImportSites importSites) {
    this.importSites = importSites;
  }

  private void extractAddressPoints() {
    final Path inputFile = this.importSites.getDirectory().resolve("CanData.csv");
    final Path inputDirectory = this.importSites.getInputDirectory();
    final Path outFile = inputDirectory.resolve("GEOCODER_CA_ADDRESS_POINT.tsv");
    final Path bcErrorFile = inputDirectory.resolve("GEOCODER_CA_ADDRESS_POINT_IN_BC.tsv");
    final Path outsideBcErrorFile = inputDirectory
      .resolve("GEOCODER_CA_ADDRESS_POINT_OUTSIDE_BC.tsv");
    final GeometryFactory geometryFactory = GeometryFactory.floating(EpsgId.NAD83, 2);
    final MapEx readProperties = new LinkedHashMapEx() //
      .add("pointXFieldName", "Longitude")//
      .add("pointYFieldName", "Latitude")//
      .add("geometryFactory", geometryFactory) //
    ;
    final GeometryFactory albersGeometryFactory = Gba.GEOMETRY_FACTORY_2D_1M;
    try (
      RecordReader reader = RecordReader.newRecordReader(inputFile, readProperties)) {
      final RecordDefinition writeRecordDefinition = new RecordDefinitionBuilder(reader) //
        .setGeometryFactory(albersGeometryFactory) //
        .getRecordDefinition();
      try (
        RecordWriter writer = RecordWriter.newRecordWriter(writeRecordDefinition, outFile);
        RecordWriter bcErrorWriter = RecordWriter.newRecordWriter(writeRecordDefinition,
          bcErrorFile);
        RecordWriter outsideBcErrorWriter = RecordWriter.newRecordWriter(writeRecordDefinition,
          outsideBcErrorFile);) {
        for (final Record record : cancellable(reader)) {
          this.counts.addCount(record, CountNames.READ);
          final Point point = record.getGeometry();
          final Point albersPoint = point.convertGeometry(albersGeometryFactory);
          final Identifier regionalDistrictId = REGIONAL_DISTRICTS.getBoundaryId(albersPoint);
          final String province = record.getString("Province");
          final boolean bcProvinceValue = "BC".equalsIgnoreCase(province);
          if (regionalDistrictId == null
            || this.REGIONAL_DISTRICTS_OUTSIDE_BC.contains(regionalDistrictId)) {
            if (bcProvinceValue) {
              this.counts.addCount(record, OUTSIDE_BC);
              final Record writeRecord = writeRecordDefinition.newRecord(record);
              writeRecord.setGeometryValue(albersPoint);
              outsideBcErrorWriter.write(writeRecord);
            }
          } else {
            if (!bcProvinceValue) {
              this.counts.addCount(record, INSIDE_BC);
              final Record writeRecord = writeRecordDefinition.newRecord(record);
              writeRecord.setGeometryValue(albersPoint);
              bcErrorWriter.write(writeRecord);
            }
            this.counts.addCount(record, CountNames.WRITE);
            final Record writeRecord = writeRecordDefinition.newRecord(record);
            writeRecord.setValue("Province", "BC");
            writeRecord.setGeometryValue(albersPoint);
            writer.write(writeRecord);

          }
        }
      }
    }
  }

  @Override
  public boolean isCancelled() {
    return this.importSites.isCancelled();
  }

  public void run() {
    this.counts = this.importSites.labelCounts(EXTRACT_BC, "File",
      CountNames.READ, INSIDE_BC, OUTSIDE_BC, CountNames.WRITE);
    SwingUtilities.invokeLater(() -> {
      this.counts.getTable().setSortOrder(0, SortOrder.ASCENDING);

      this.importSites.setSelectedTab(EXTRACT_BC);
    });
    // extractPostalCodePoints();
    extractAddressPoints();
  }

}
