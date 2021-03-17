package ca.bc.gov.gbasites.load.common;

import java.util.Collections;
import java.util.List;

import org.jeometry.common.data.identifier.Identifier;

import ca.bc.gov.gba.itn.model.code.GbaItnCodeTables;
import ca.bc.gov.gba.itn.model.code.StructuredNames;

import com.revolsys.util.Strings;

public class StructuredNameMapping {
  public static final StructuredNames STRUCTURED_NAMES = GbaItnCodeTables.getStructuredNames();

  private static final String STREET_NAME_NOT_FOUND = "STREET_NAME not found";

  private final String originalStructuredName;

  private String structuredName;

  private Identifier structuredNameId;

  private String message = STREET_NAME_NOT_FOUND;

  public StructuredNameMapping(final String originalStructuredName, final String structuredName) {
    this.originalStructuredName = originalStructuredName;
    setStructuredName(STREET_NAME_NOT_FOUND, true, structuredName);
  }

  public boolean contains(final String text) {
    return this.structuredName.contains(text);
  }

  public boolean endsWith(final String text) {
    return this.structuredName.endsWith(text);
  }

  public String getCurrentStructuredName() {
    return this.structuredName;
  }

  public String getMatchedStructuredName() {
    if (this.structuredNameId == null) {
      return null;
    } else {
      return this.structuredName;
    }
  }

  public String getMessage() {
    return this.message;
  }

  public String getOriginalStructuredName() {
    return this.originalStructuredName;
  }

  public Identifier getStructuredNameId() {
    return this.structuredNameId;
  }

  public boolean isExactMatch() {
    return this.originalStructuredName.equalsIgnoreCase(this.structuredName);
  }

  public boolean isMatched() {
    return this.structuredNameId != null;
  }

  public boolean isNotMatched() {
    return this.structuredNameId == null;
  }

  public boolean matchReplace(final String message, final boolean forceName, final String from,
    final String to) {
    if (isMatched()) {
      return true;
    } else {
      final String newName = this.structuredName.replace(from, to);
      if (setStructuredName(message, forceName, newName)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean matchReplaceAll(final String message, final boolean forceName, final String from,
    final String to) {
    if (isMatched()) {
      return true;
    } else {
      final String newName = this.structuredName.replaceAll(from, to);
      if (setStructuredName(message, forceName, newName)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean matchReplaceFirst(final String message, final boolean forceName, final String from,
    final String to) {
    if (isMatched()) {
      return true;
    } else {
      final String newName = this.structuredName.replaceFirst(from, to);
      if (setStructuredName(message, forceName, newName)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean matchSuffix(final String message, final boolean forceName, final String suffix) {
    if (isMatched()) {
      return true;
    } else {
      final String newName = this.structuredName + suffix;
      if (setStructuredName(message, forceName, newName)) {
        return true;
      } else {
        return false;
      }
    }
  }

  public boolean setStructuredName(final String message, final boolean forceName,
    final Object... nameParts) {
    final String name = Strings.toString(" ", nameParts);
    return setStructuredName(message, forceName, name);
  }

  /**
   * Set the structured name if a match is found.
   * @param message
   * @param structuredName
   * @return
   */
  public boolean setStructuredName(final String message, final boolean forceName,
    String structuredName) {
    structuredName = Strings.cleanWhitespace(structuredName);
    final Identifier structuredNameId = STRUCTURED_NAMES
      .getIdentifier(Collections.singletonList(structuredName), false);
    if (setStructuredNameId(structuredNameId, message)) {
      return true;
    } else {
      if (forceName) {
        this.structuredName = structuredName.toUpperCase();
      }
      return false;
    }
  }

  public boolean setStructuredNameId(final Identifier structuredNameId, final String message) {
    if (structuredNameId == null) {
      return isMatched();
    } else {
      this.structuredNameId = structuredNameId;
      this.structuredName = STRUCTURED_NAMES.getValue(structuredNameId);
      this.message = message;
      return true;
    }
  }

  public boolean setUsingSimplifiedName() {
    final List<Identifier> structuredNameIds = STRUCTURED_NAMES
      .getMatchingNameIds(this.structuredName);
    if (structuredNameIds.isEmpty()) {
      STRUCTURED_NAMES.getMatchingNameIds(this.structuredName);
      return false;
    } else if (structuredNameIds.size() == 1) {
      final Identifier structuredNameId = structuredNameIds.get(0);
      setStructuredNameId(structuredNameId, "STREET_NAME has different spelling");
      return true;
    } else {
      this.message = "Multiple matching STRUCTURED_NAME records";
      return false;
    }
  }

  public boolean startsWith(final String text) {
    return this.structuredName.startsWith(text);
  }

  @Override
  public String toString() {
    if (isExactMatch()) {
      return this.originalStructuredName;
    } else {
      return this.originalStructuredName + "\t" + this.structuredName;
    }
  }
}
