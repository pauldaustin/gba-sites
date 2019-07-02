package ca.bc.gov.gbasites.controller;

import com.revolsys.util.ManifestUtil;
import com.revolsys.util.Property;

public class GbaSiteController {

  public static String getVersion() {
    final String version = ManifestUtil.getImplementationVersion("GeoBC Atlas Sites");
    if (Property.hasValue(version)) {
      return version;
    } else {
      return "0.0.0-TBA";
    }
  }

}
