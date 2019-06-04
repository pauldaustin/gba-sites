package ca.bc.gov.gbasites.model.type.code;

public interface SiteType {
  String VIRTUAL = "VRT_";

  String VIRTUAL_BLOCK_FROM = "VRT_BLF";

  String VIRTUAL_BLOCK_SPLIT = "VRT_BLS";

  String VIRTUAL_BLOCK_TO = "VRT_BLT";

  static String getVirtualSiteTypeCode(final Integer civicNumber) {
    if (civicNumber == null) {
      return null;
    } else {
      final int number = Math.abs(civicNumber) % 100;
      if (number < 2 || civicNumber == 2) {
        return VIRTUAL_BLOCK_FROM;
      } else if (number > 97) {
        return VIRTUAL_BLOCK_TO;
      } else {
        return VIRTUAL_BLOCK_SPLIT;
      }
    }
  }
}
