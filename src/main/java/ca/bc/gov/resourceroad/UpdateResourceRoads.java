package ca.bc.gov.resourceroad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jeometry.common.data.identifier.Identifier;
import org.jeometry.common.io.PathName;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.itn.model.GbaItnTables;
import ca.bc.gov.gba.itn.model.GbaType;
import ca.bc.gov.gba.itn.model.TransportLine;
import ca.bc.gov.gba.itn.model.TransportLineType;
import ca.bc.gov.gba.model.type.TransportLines;
import ca.bc.gov.gba.ui.BatchUpdateDialog;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;

import com.revolsys.collection.map.LinkedHashMapEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.collection.map.Maps;
import com.revolsys.geometry.algorithm.LineStringLocation;
import com.revolsys.geometry.algorithm.VertexHausdorffDistance;
import com.revolsys.geometry.graph.Edge;
import com.revolsys.geometry.graph.Node;
import com.revolsys.geometry.graph.RecordGraph;
import com.revolsys.geometry.model.BoundingBox;
import com.revolsys.geometry.model.LineString;
import com.revolsys.geometry.model.Lineal;
import com.revolsys.geometry.model.Point;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Transaction;
import com.revolsys.util.Debug;

public class UpdateResourceRoads extends BatchUpdateDialog implements TransportLine {
  private static final long serialVersionUID = 1L;

  private static final String CLASS_NAME = UpdateResourceRoads.class.getSimpleName();

  private static final String APPLICATION_STATUS_CODE = "APPLICATION_STATUS_CODE";

  private static final String ACTIVE = "Active";

  private static final String MISSING = "Missing";

  private static final String FOREST_FILE_ID = "FOREST_FILE_ID";

  private static final PathName FTEN_ROAD_LINES = PathName
    .newPathName("/WHSE_FOREST_TENURE/FTEN_ROAD_LINES");

  private static final String RETIRED = "Retired";

  private static final String RETIREMENT_DATE = "RETIREMENT_DATE";

  private static final String SPLIT = "Split";

  private static final String MOVED_NODE = "Moved Node";

  private static final String ROAD_NAME = "ROAD_NAME";

  private static final String ROAD_SECTION_ID = "ROAD_SECTION_ID";

  private static final List<String> FTEN_ID_FIELD_NAMES = Arrays.asList(FOREST_FILE_ID,
    ROAD_SECTION_ID);

  private static final String MATCHED = "Matched";

  public static void main(final String[] args) {
    start(UpdateResourceRoads.class);
  }

  private final Set<Identifier> resourceRecordRetiredIds = new HashSet<>();

  private final Set<Identifier> resourceRecordActiveIds = new HashSet<>();

  private final List<Record> resourceRecords = new ArrayList<>();

  private final Map<Identifier, List<Record>> gbaRecordsByResourceRecordId = new LinkedHashMap<>();

  final RecordStore gbaRecordStore = GbaSiteDatabase.getRecordStore();

  public UpdateResourceRoads() {
    super(CLASS_NAME, READ, MISSING, RETIRED, ACTIVE, MATCHED, INSERTED, UPDATED, DELETED, ERROR);
    newLabelCount(COUNTS, FTEN_ROAD_LINES, READ);
    newLabelCount(COUNTS, GbaItnTables.TRANSPORT_LINE, READ);

  }

  @Override
  protected boolean batchUpdate(final Transaction transaction) {
    try (
      RecordStore bcgwRecordStore = GbaController.newBcgwRecordStore()) {
      loadResourceRetiredRoads(bcgwRecordStore);
      loadResourceActiveRoads(bcgwRecordStore);

      processExistingGbaResourceRecords(this.gbaRecordStore);

      processRoadsByFileId();
    }
    return true;
  }

  private void clearResourceFields(final RecordStore gbaRecordStore, final Record record,
    final String countName) {
    addLabelCount(COUNTS, record, countName);
    final MapEx newValues = new LinkedHashMapEx();
    for (final String fieldName : RESOURCE_ROAD_FIELD_NAMES) {
      newValues.put(fieldName, null);
    }
    try (
      Transaction transaction = gbaRecordStore.newTransaction()) {
      updateRecord(COUNTS, record, newValues);
    }
  }

  private void gbaRecordInsert(final Record resourceRecord, final LineString resourceLine) {
    final Record newRecord = this.gbaRecordStore.newRecord(GbaItnTables.TRANSPORT_LINE);
    newRecord.setValues(TransportLine.NON_DEMOGRAPHIC_FIXED_FIELD_VALUES);
    newRecord.setValue(DATA_CAPTURE_METHOD_CODE, "unknown");
    newRecord.setValue(TRANSPORT_LINE_TYPE_CODE, TransportLineType.ROAD_RESOURCE);
    newRecord.setValue(TRANSPORT_LINE_SURFACE_CODE, "unknown");
    newRecord.setValue(LEFT_NUMBER_OF_LANES, 0);
    newRecord.setValue(RIGHT_NUMBER_OF_LANES, 0);
    newRecord.setValue(TOTAL_NUMBER_OF_LANES, 1);
    newRecord.setGeometryValue(resourceLine);
    setResourceValues(newRecord, resourceRecord);
    insertRecord(COUNTS, newRecord);
  }

  private void gbaRecordsSplitWithinDistanceOfResourceNode(final RecordGraph resourceRecordGraph) {
    if (!isCancelled()) {
      final Query query = new Query(GbaItnTables.TRANSPORT_LINE);
      query.setWhereCondition(
        Q.in(TRANSPORT_LINE_TYPE_CODE, TransportLines.getTypeCodeNonDemographic()));
      try (
        RecordReader reader = this.gbaRecordStore.getRecords(query)) {
        for (final Record gbaRecord : cancellable(this.gbaRecordStore.getRecords(query))) {
          gbaRecordsSplitWithinDistanceOfResourceNode(resourceRecordGraph, gbaRecord);
        }
      }
    }
  }

  private void gbaRecordsSplitWithinDistanceOfResourceNode(final RecordGraph resourceRecordGraph,
    final Record gbaRecord) {
    final List<LineStringLocation> splitLocations = new ArrayList<>();
    final LineString gbaLine = gbaRecord.getGeometry();
    final BoundingBox boundingBox = gbaLine //
      .bboxEditor() //
      .expandDelta(3);
    resourceRecordGraph.forEachNode(boundingBox, (resourceNode) -> {
      final double x = resourceNode.getX();
      final double y = resourceNode.getY();
      final LineStringLocation location = gbaLine.getLineStringLocation(x, y);
      final int maxDistance = 3;
      if (location != null && !location.isFromVertex() && !location.isToVertex()
        && location.getDistance() < maxDistance) {
        Point splitPoint;
        final int vertexCount = gbaLine.getVertexCount();
        if (location.isVertex()) {
          splitPoint = location.getPoint2d();
        } else {
          final int segmentIndex = location.getSegmentIndex();
          final double distanceFrom = gbaLine.distanceVertex(segmentIndex, x, y);
          if (distanceFrom < maxDistance) {
            if (segmentIndex == 0) {
              return;
            } else {
              splitPoint = gbaLine.getPoint(segmentIndex);
            }
          } else {
            final double distanceTo = gbaLine.distanceVertex(segmentIndex + 1, x, y);
            if (distanceTo < maxDistance) {
              if (segmentIndex + 1 == vertexCount - 1) {
                return;
              } else {
                splitPoint = gbaLine.getPoint(segmentIndex + 1);
              }
            } else {
              splitPoint = location.getPoint2d();
            }
          }
        }
        if (gbaLine.distanceVertex(0, splitPoint) < 10) {
          // Don't split if < 10m from the start of the line
        } else if (gbaLine.distanceVertex(vertexCount - 1, splitPoint) < 10) {
          // Don't split if < 10m from the end of the line
        } else {
          splitLocations.add(location);
        }

      }
    });
    if (!splitLocations.isEmpty()) {
      final List<LineString> newLines = gbaLine.split(splitLocations);
      if (newLines.size() > 1) {
        for (final LineString newLine : newLines) {
          final Record newRecord = gbaRecord.newRecordGeometry(newLine);
          newRecord.setIdentifier(null);
          insertRecord(COUNTS, newRecord);
          addLabelCount(COUNTS, GbaItnTables.TRANSPORT_LINE, SPLIT);
        }
        deleteRecord(gbaRecord);
      }

    }
  }

  private void loadResourceActiveRoads(final RecordStore bcgwRecordStore) {
    final Query retiredQuery = new Query(FTEN_ROAD_LINES)//
      .select(FOREST_FILE_ID, ROAD_SECTION_ID, ROAD_NAME, GbaType.GEOMETRY) //
      .setWhereCondition(Q.and(//
        Q.isNull(RETIREMENT_DATE), //
        Q.equal(APPLICATION_STATUS_CODE, "A") //
      ))//
      .setOrderByFieldNames(FOREST_FILE_ID, ROAD_SECTION_ID) //
    ;
    try (
      RecordReader records = bcgwRecordStore.getRecords(retiredQuery)) {
      for (final Record record : records) {
        addLabelCount(COUNTS, record, READ);
        addLabelCount(COUNTS, record, ACTIVE);
        final Identifier identidier = record.getIdentifier(FTEN_ID_FIELD_NAMES);
        this.resourceRecordActiveIds.add(identidier);
        this.resourceRecords.add(record);
      }
    }
  }

  private void loadResourceRetiredRoads(final RecordStore bcgwRecordStore) {
    final Query retiredQuery = new Query(FTEN_ROAD_LINES)//
      .select(FOREST_FILE_ID, ROAD_SECTION_ID) //
      .setWhereCondition(Q.isNotNull(RETIREMENT_DATE)) //
    ;
    try (
      RecordReader records = bcgwRecordStore.getRecords(retiredQuery)) {
      for (final Record record : records) {
        addLabelCount(COUNTS, record, READ);
        addLabelCount(COUNTS, record, RETIRED);
        final Identifier identidier = record.getIdentifier(FTEN_ID_FIELD_NAMES);
        this.resourceRecordRetiredIds.add(identidier);
      }
    }
  }

  private void matchGbaRecords(final RecordGraph resourceRecordGraph) {
    if (!isCancelled()) {
      final Query query = new Query(GbaItnTables.TRANSPORT_LINE);
      query.select(GbaType.GEOMETRY);
      try (
        RecordReader reader = this.gbaRecordStore.getRecords(query)) {
        for (final Record gbaRecord : cancellable(this.gbaRecordStore.getRecords(query))) {
          final LineString gbaLine = gbaRecord.getGeometry();
          final Point fromPoint = gbaLine.getFromPoint();
          resourceRecordsEdgeSliptPoint(resourceRecordGraph, fromPoint);

          final Point toPoint = gbaLine.getToPoint();
          resourceRecordsEdgeSliptPoint(resourceRecordGraph, toPoint);
        }
      }
    }
    gbaRecordsSplitWithinDistanceOfResourceNode(resourceRecordGraph);
    GbaController.forEach100000Tile((boundingBox) -> {
      if (!isCancelled()) {
        final Query query = Query.intersects(this.gbaRecordStore, GbaItnTables.TRANSPORT_LINE,
          boundingBox);//
        final RecordGraph gbaRecordGraph = new RecordGraph();
        try (
          RecordReader reader = this.gbaRecordStore.getRecords(query)) {
          for (final Record gbaRecord : cancellable(this.gbaRecordStore.getRecords(query))) {
            gbaRecordGraph.addEdge(gbaRecord);
          }
        }

        resourceRecordGraph.forEachEdge(boundingBox, (resourceEdge) -> {
          final Record resourceRecord = resourceEdge.getObject();
          final LineString resourceLine = resourceRecord.getGeometry();
          if (boundingBox.bboxCovers(resourceLine.getX(0), resourceLine.getY(0))) {
            matchGbaRecordsNoMatching(gbaRecordGraph, resourceRecord, resourceLine);
            matchGbaRecordsEqual(gbaRecordGraph, resourceRecord, resourceLine);
            matchGbaRecordsClose(gbaRecordGraph, resourceRecord, resourceLine);
          }
        });

        // gbaRecordGraph.forEachEdge(boundingBox, (gbaEdge) -> {
        // final Record gbaRecord = gbaEdge.getObject();
        // final LineString gbaLine = gbaRecord.getGeometry();
        // if (boundingBox.covers(gbaLine.getX(0), gbaLine.getY(0))) {
        // matchResourceRecordsCloseSubline(resourceRecordGraph, gbaRecord,
        // gbaLine);
        // }
        // });

      }
    });
  }

  private void matchGbaRecordsClose(final RecordGraph gbaRecordGraph, final Record resourceRecord,
    final LineString resourceLine) {

    final BoundingBox searchBoundingBox = resourceLine.getBoundingBox() //
      .bboxEditor() //
      .expandDelta(20);
    final List<Edge<Record>> gbaEdges = gbaRecordGraph.getEdges(searchBoundingBox, (gbaEdge) -> {
      final Record gbaRecord = gbaEdge.getObject();
      final LineString gbaLine = gbaRecord.getGeometry();

      final double distance = VertexHausdorffDistance.distance(resourceLine, gbaLine);
      if (distance < 50) {
        return true;
      } else {
        return false;
      }
    });
    if (gbaEdges.size() == 1) {
      final Edge<Record> gbaEdge = gbaEdges.get(0);
      setMatched(gbaEdge, resourceRecord);
    } else if (!gbaEdges.isEmpty()) {
      Debug.noOp();
    } else {
      Debug.noOp();
    }
  }

  private void matchGbaRecordsEqual(final RecordGraph gbaRecordGraph, final Record resourceRecord,
    final LineString resourceLine) {
    final List<Edge<Record>> gbaEdges = gbaRecordGraph.getEdges(resourceLine, (gbaEdge) -> {
      final Record gbaRecord = gbaEdge.getObject();
      final LineString gbaLine = gbaRecord.getGeometry();
      if (gbaLine.equals(resourceLine)) {
        return true;
      } else {
        return false;
      }
    });
    for (final Edge<Record> gbaEdge : gbaEdges) {
      setMatched(gbaEdge, resourceRecord);
    }
  }

  private void matchGbaRecordsNoMatching(final RecordGraph gbaRecordGraph,
    final Record resourceRecord, final LineString resourceLine) {

    final BoundingBox searchBoundingBox = resourceLine.getBoundingBox() //
      .bboxEditor() //
      .expandDelta(20);
    final List<Edge<Record>> gbaEdges = gbaRecordGraph.getEdges(searchBoundingBox);
    if (gbaEdges.isEmpty()) {
      gbaRecordInsert(resourceRecord, resourceLine);
      // addLabelCount(COUNTS, GbaItnTables.TRANSPORT_LINE, INSERTED);
    } else {
      boolean hasEdgeMatch = false;
      for (final Edge<Record> gbaEdge : gbaEdges) {
        final Record gbaRecord = gbaEdge.getObject();
        final LineString gbaLine = gbaRecord.getGeometry();
        if (gbaLine.getLength() > 10) {
          final LineString nearestLine = resourceLine.getMaximalNearestSubline(gbaLine);
          if (nearestLine.getLength() > 10) {
            hasEdgeMatch = true;
          }
        } else {
          hasEdgeMatch = true;
        }
      }
      if (!hasEdgeMatch) {
        gbaRecordInsert(resourceRecord, resourceLine);
      }
    }
  }

  private void processExistingGbaResourceRecords(final RecordStore gbaRecordStore) {
    final Query query = new Query(GbaItnTables.TRANSPORT_LINE)//
      .setWhereCondition(//
        Q.and(//
          Q.isNotNull(RESOURCE_ROAD_FILE_ID), //
          Q.isNotNull(RESOURCE_ROAD_SECTION_ID) //
        ))//
    ;
    try (
      RecordReader records = gbaRecordStore.getRecords(query)) {
      for (final Record record : cancellable(records)) {
        final Identifier resourceRecordIdentifier = record
          .getIdentifier(RESOURCE_ROAD_ID_FIELD_NAMES);
        addLabelCount(COUNTS, record, READ);
        if (this.resourceRecordRetiredIds.contains(resourceRecordIdentifier)) {
          clearResourceFields(gbaRecordStore, record, RETIRED);
        } else {
          if (this.resourceRecordActiveIds.contains(resourceRecordIdentifier)) {
            Maps.addToList(this.gbaRecordsByResourceRecordId, resourceRecordIdentifier, record);
          } else {
            clearResourceFields(gbaRecordStore, record, MISSING);
          }
        }
      }
    }
  }

  private void processRoadsByFileId() {
    snapResourceRecords();
  }

  private void resourceRecordsEdgeSliptPoint(final RecordGraph resourceRecordGraph,
    final Point point) {
    final List<Edge<Record>> closeEdges = resourceRecordGraph.getEdges(point, 10);
    closeEdges.removeIf((edge) -> {
      return edge.hasNode(point);
    });
    if (closeEdges.size() == 1) {
      final Edge<Record> closeEdge = closeEdges.get(0);
      addLabelCount(COUNTS, FTEN_ROAD_LINES, SPLIT);
      closeEdge.splitEdge(point);
    }
  }

  private void resourceRecordsNodesSnapNodesWithinDistance(final RecordGraph resourceRecordGraph,
    final RecordGraph nodeGraph) {
    // System.out.println(resourceRecordGraph.getNodeCount());
    nodeGraph.forEachNode((node) -> {
      final List<Node<Record>> closeNodes = resourceRecordGraph.getNodes(node, 10);
      if (closeNodes.size() > 1) {
        double sumX = 0;
        long sumY = 0;
        for (final Node<Record> closeNode : closeNodes) {
          final double x = closeNode.getX();
          final double y = closeNode.getY();
          sumX += Math.round(x);
          sumY += Math.round(y);
        }
        final double newX = Math.round(sumX / closeNodes.size());
        final double newY = Math.round(sumY / closeNodes.size());
        for (final Node<Record> node2 : closeNodes) {
          node2.moveNode(newX, newY);
          addLabelCount(COUNTS, FTEN_ROAD_LINES, MOVED_NODE);
        }
      }
    });
    // System.out.println(resourceRecordGraph.getNodeCount());
  }

  private void setMatched(final Edge<Record> gbaEdge, final Record resourceRecord) {
    final Record gbaRecord = gbaEdge.getObject();
    final MapEx newValues = new LinkedHashMapEx();
    setResourceValues(newValues, resourceRecord);
    updateRecord(COUNTS, gbaRecord, newValues);
    addLabelCount(COUNTS, gbaRecord, MATCHED);
  }

  private void setResourceValues(final MapEx newValues, final Record resourceRecord) {
    newValues.put(RESOURCE_ROAD_FILE_ID, resourceRecord.getValue(FOREST_FILE_ID));
    newValues.put(RESOURCE_ROAD_SECTION_ID, resourceRecord.getValue(ROAD_SECTION_ID));
    newValues.put(RESOURCE_ROAD_NAME, resourceRecord.getValue(ROAD_NAME));
  }

  private void snapResourceRecords() {
    final RecordGraph resourceRecordGraph = new RecordGraph();
    for (final Record resourceRecord : this.resourceRecords) {
      final Lineal lines = resourceRecord.getGeometry();
      if (lines.isGeometryCollection()) {
        for (final LineString line : lines.lineStrings()) {
          final Record resourceRecordCopy = resourceRecord.newRecordGeometry(line);
          resourceRecordGraph.addEdge(resourceRecordCopy);
        }
      } else {
        resourceRecordGraph.addEdge(resourceRecord);
      }
    }
    resourceRecordsNodesSnapNodesWithinDistance(resourceRecordGraph, resourceRecordGraph);
    snapResourceRecordsEdgesWithinDistance(resourceRecordGraph, resourceRecordGraph);
    matchGbaRecords(resourceRecordGraph);
    System.out.println("Resource Road Segment Count\t" + resourceRecordGraph.getEdgeCount());
  }

  private void snapResourceRecordsEdgesWithinDistance(final RecordGraph resourceRecordGraph,
    final RecordGraph nodeGraph) {
    nodeGraph.forEachNode((node) -> {
      resourceRecordsEdgeSliptPoint(resourceRecordGraph, node);
    });
  }
}
