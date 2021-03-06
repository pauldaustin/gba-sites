package ca.bc.gov.gbasites.load.provider.addressbc;

import java.nio.file.Path;

import org.jeometry.common.io.PathName;

import ca.bc.gov.gba.itn.model.code.PartnerOrganization;
import ca.bc.gov.gbasites.controller.GbaSiteDatabase;
import ca.bc.gov.gbasites.model.type.SitePoint;

public interface AddressBC {

  String NAME = "AddressBC";

  String ICI_SOCIETY = "ICI Society";

  PartnerOrganization PARTNER_ORGANIZATION = GbaSiteDatabase.newPartnerOrganization(ICI_SOCIETY);

  String COUNT_PREFIX = "A ";

  Path DIRECTORY = SitePoint.SITES_DIRECTORY.resolve(NAME);

  String ADDRESS_POINT_TYPE = "ADDRESS_POINT_TYPE";

  String BUILDING_NAME = "BUILDING_NAME";

  String BUILDING_TYPE = "BUILDING_TYPE";

  PathName CIVIC_ADDRESS = PathName.newPathName("/CIVIC_ADDRESS");

  String CIVIC_ID = "CIVIC_ID";

  String GEOGRAPHIC_DOMAIN = "GEOGRAPHIC_DOMAIN";

  String ISSUING_AGENCY = "ISSUING_AGENCY";

  String LOCALITY = "LOCALITY";

  String LOCALITY_NAME_ALIAS = "LOCALITY_NAME_ALIAS";

  String MUNI_ID = "MUNI_ID";

  String POSTAL_CODE = "POSTAL_CODE";

  String REGIONAL_DISTRICT = "REGIONAL_DISTRICT";

  String STREET_DIR_PREFIX = "STREET_DIR_PREFIX";

  String STREET_DIR_SUFFIX = "STREET_DIR_SUFFIX";

  String FULL_ADDRESS = "FULL_ADDRESS";

  String STREET_NAME = "STREET_NAME";

  String STREET_NUMBER = "STREET_NUMBER";

  String STREET_NUMBER_PREFIX = "STREET_NUMBER_PREFIX";

  String STREET_NUMBER_RANGE = "STREET_NUMBER_RANGE";

  String STREET_NUMBER_SUFFIX = "STREET_NUMBER_SUFFIX";

  String STREET_TYPE = "STREET_TYPE";

  String UNIT_NUMBER = "UNIT_NUMBER";

  String UNIT_NUMBER_SUFFIX = "UNIT_NUMBER_SUFFIX";

  String UNIT_RANGE = "UNIT_RANGE";

  String UNIT_TYPE = "UNIT_TYPE";

  String FILE_SUFFIX = "_ABC";

  String PROVIDER_ICI_SOCIETY = "Provider - ICI Society";

}
