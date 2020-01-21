package ca.bc.gov.gbasites.ui.form;

import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.beans.PropertyChangeEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.swing.JPanel;

import ca.bc.gov.gba.model.type.GbaType;
import ca.bc.gov.gba.ui.form.SessionRecordForm;
import ca.bc.gov.gba.ui.form.StructuredNamesPanel;
import ca.bc.gov.gba.ui.layer.SessionProxyLayerRecord;
import ca.bc.gov.gba.ui.layer.SessionRecordLayer;
import ca.bc.gov.gbasites.model.type.SitePoint;

import com.revolsys.io.BaseCloseable;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.field.Field;
import com.revolsys.swing.map.layer.record.LayerRecord;
import com.revolsys.swing.map.layer.record.component.RecordLayerQueryTextField;
import com.revolsys.swing.undo.UndoManager;

public class SitePointForm extends SessionRecordForm implements FocusListener {
  private static final long serialVersionUID = 1L;

  private StructuredNamesPanel namesPanel;

  public SitePointForm(final SessionRecordLayer layer, final SessionProxyLayerRecord record) {
    super(layer);
    setAllowAddWithErrors(true);
    addTabs();
    setRecord(record);
  }

  private void addStructuredNamesPanel(final JPanel panel) {
    this.namesPanel = new StructuredNamesPanel(this, 2);
    final List<Field> fields = this.namesPanel.getFields();
    addFields(fields);
    panel.add(this.namesPanel);
  }

  private void addTabAddress() {
    final JPanel panel = addScrollPaneTab(2, "Address & Names");

    addPanel(panel, "Address",
      Arrays.asList(SitePoint.FULL_ADDRESS, SitePoint.UNIT_DESCRIPTOR, SitePoint.CIVIC_NUMBER,
        SitePoint.CIVIC_NUMBER_SUFFIX, SitePoint.USE_IN_ADDRESS_RANGE_IND,
        SitePoint.ADDRESS_COMMENT));

    addStructuredNamesPanel(panel);

  }

  private void addTabBoundaries() {
    final JPanel panel = addScrollPaneTab(3, "Boundary");

    addPanel(panel, "Boundaries",
      Arrays.asList(SitePoint.LOCALITY_ID, SitePoint.COMMUNITY_ID, SitePoint.REGIONAL_DISTRICT_ID));

  }

  private void addTabMetaData() {
    final JPanel panel = addScrollPaneTab(0, "Metadata");

    addPanel(panel, "Metadata", Arrays.asList("OBJECTID", SitePoint.SITE_ID,
      SitePoint.PARENT_SITE_ID, SitePoint.TRANSPORT_LINE_ID));
    addPanel(panel, "Organizations", Arrays.asList(GbaType.CREATE_PARTNER_ORG_ID,
      GbaType.MODIFY_PARTNER_ORG_ID, GbaType.CUSTODIAN_PARTNER_ORG_ID));
    addPanel(panel, "Capture", Arrays.asList(GbaType.CAPTURE_DATE, GbaType.FEATURE_STATUS_CODE));
  }

  private void addTabs() {
    addTabMetaData();
    addTabSite();
    addTabAddress();
    addTabBoundaries();
    getTabs().setSelectedIndex(0);
  }

  private void addTabSite() {
    final JPanel panel = addScrollPaneTab(1, "Site");

    addPanel(panel, "Site Type", Arrays.asList(SitePoint.SITE_TYPE_CODE,
      SitePoint.SITE_LOCATION_CODE, SitePoint.EMERGENCY_MANAGEMENT_SITE_IND));

    addPanel(panel, "Site Names", Arrays.asList(SitePoint.USE_SITE_NAME_IN_ADDRESS_IND,
      SitePoint.SITE_NAME_1, SitePoint.SITE_NAME_2, SitePoint.SITE_NAME_3));
  }

  @Override
  protected void configureFields() {
    super.configureFields();
    getFieldsTableModel().setEditable(false);
    final RecordLayerQueryTextField parentSiteField = new RecordLayerQueryTextField(
      SitePoint.PARENT_SITE_ID, getLayer(), SitePoint.FULL_ADDRESS);
    parentSiteField.setMinSearchCharacters(5);
    addField(parentSiteField);
    addField(SwingUtil.newTextArea(SitePoint.FULL_ADDRESS, 5, 60));
    setReadOnlyFieldNames(SitePoint.FULL_ADDRESS);

  }

  @Override
  public void destroy() {
    super.destroy();
    this.namesPanel = null;
  }

  @Override
  public void focusGained(final FocusEvent e) {
    super.focusGained(e);
    if (this.namesPanel != null) {
      this.namesPanel.updateStructuredNameButtons();
    }
  }

  @Override
  public void focusLost(final FocusEvent e) {
    super.focusLost(e);
    if (this.namesPanel != null) {
      this.namesPanel.updateStructuredNameButtons();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getFieldValue(final String name) {
    if (name.startsWith("STRUCTURED_NAME")) {
      if (this.namesPanel == null) {
        return null;
      } else {
        final int index = Integer.parseInt(name.substring(16, 17)) - 1;
        final List<Integer> nameIds = this.namesPanel.getNameIds();
        if (index < nameIds.size()) {
          return (T)nameIds.get(index);
        } else {
          return null;
        }
      }
    } else {
      final T value = super.getFieldValue(name);
      return value;
    }
  }

  @Override
  public Map<String, Object> getValues() {
    final Map<String, Object> fieldValues = super.getValues();
    if (this.namesPanel != null) {
      final List<Integer> nameIds = this.namesPanel.getNameIds();
      for (int i = 0; i < 2; i++) {
        Integer nameId = null;
        if (i < nameIds.size()) {
          nameId = nameIds.get(i);
        }
        fieldValues.put("STRUCTURED_NAME_" + (i + 1) + "_ID", nameId);
      }
    }
    return fieldValues;
  }

  @Override
  public void propertyChange(final PropertyChangeEvent event) {
    super.propertyChange(event);
    final Object source = event.getSource();
    if (isSame(source)) {
      final String propertyName = event.getPropertyName();
      if (propertyName.startsWith("STRUCTURED_NAME")) {
        this.namesPanel.updateNames();
      }
    }
  }

  @Override
  protected void setFieldInvalidDo(final String fieldName, final String message) {
    super.setFieldInvalidDo(fieldName, message);
    if (fieldName.startsWith("STRUCTURED_NAME")) {
      if (this.namesPanel != null) {
        this.namesPanel.setFieldValid(false);
      }
    }
  }

  @Override
  protected void setFieldValidDo(final String fieldName) {
    if (fieldName.startsWith("STRUCTURED_NAME")) {
      if (this.namesPanel != null) {
        this.namesPanel.setFieldValid(true);
      }
    }
    super.setFieldValidDo(fieldName);
  }

  @Override
  protected void setRecordDo(final LayerRecord record) {
    final UndoManager undoManager = getUndoManager();
    try (
      final BaseCloseable cu = undoManager.setEventsEnabled(false);
      final BaseCloseable c = setFieldValidationEnabled(false)) {
      super.setRecordDo(record);
      if (this.namesPanel != null) {
        this.namesPanel.updateNames();
        this.namesPanel.updateStructuredNameButtons();
        for (final String nameField : SitePoint.SITE_STRUCTURED_NAME_FIELD_NAMES) {
          setFieldValid(nameField);
        }
        updateErrors();
      }
    }
  }
}
