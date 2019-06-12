package ca.bc.gov.gbasites;

import ca.bc.gov.gbasites.load.ImportSites;

import com.revolsys.util.ServiceInitializer;

public class GbaSitesServiceInitializer implements ServiceInitializer {

  @Override
  public void initializeService() {
    ImportSites.initializeService();
  }

  @Override
  public int priority() {
    return 10000;
  }
}
