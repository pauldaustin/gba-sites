package ca.bc.gov.gbasites.load.provider.geocoderca;

import java.nio.file.Path;

import ca.bc.gov.gbasites.load.ImportSites;

public interface GeocoderCa {

  String A_TYPE = "aType";

  String CITY = "City";

  String CONFIDENCE = "Confidence";

  String COUNT = "Count";

  String DIRECTON = "Direction";

  String LATITUDE = "Latitude";

  String LONGITUDE = "Longitude";

  String NUMBER = "Number";

  String POINT = "Point";

  String POST_CODE = "PostCode";

  String PROVINCE = "Province";

  String STREET = "Street";

  String STREET_NAME = "Street Name";

  String TYPE = "Type";

  String UNIT = "Unit";

  Path DIRECTORY = ImportSites.SITES_DIRECTORY.resolve("geocoder.ca");

}
