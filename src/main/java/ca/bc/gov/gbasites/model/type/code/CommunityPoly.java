package ca.bc.gov.gbasites.model.type.code;

import org.jeometry.common.io.PathName;

import ca.bc.gov.gba.controller.GbaController;
import ca.bc.gov.gba.model.BoundaryCache;

import com.revolsys.record.schema.RecordStore;
import com.revolsys.swing.parallel.Invoke;

public class CommunityPoly {
  public static final PathName COMMUNITY_POLY = PathName.newPathName("/GBA/COMMUNITY_POLY");

  public static BoundaryCache communities;

  public static BoundaryCache getCommunities() {
    synchronized (CommunityPoly.class) {
      if (communities == null) {
        final RecordStore recordStore = GbaController.getUserRecordStore();
        communities = recordStore.getCodeTable(COMMUNITY_POLY);
        if (communities == null) {
          communities = new BoundaryCache();
        } else {
          communities.refreshIfNeeded();
        }
      }
    }
    return communities;
  }

  public static synchronized void initCodeTables() {
    final RecordStore recordStore = GbaController.getUserRecordStore();
    if (recordStore != null) {
      if (communities == null) {
        communities = recordStore.getCodeTable(COMMUNITY_POLY);
        if (communities != null) {
          Invoke.background("Refresh " + communities, communities::refreshIfNeeded);
        }
      }
    }
  }
}
