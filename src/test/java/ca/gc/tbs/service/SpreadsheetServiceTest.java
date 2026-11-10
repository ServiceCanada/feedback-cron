package ca.gc.tbs.service;

import static org.mockito.Mockito.mockStatic;

import ca.gc.tbs.GoogleSheetsAPI;
import ca.gc.tbs.domain.Problem;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

class SpreadsheetServiceTest {

  @Test
  void localModeNeverWritesDuplicatesToGoogleSheets() {
    SpreadsheetService service = new SpreadsheetService();
    ReflectionTestUtils.setField(service, "integrationsEnabled", false);

    try (MockedStatic<GoogleSheetsAPI> sheets = mockStatic(GoogleSheetsAPI.class)) {
      service.logDuplicateComment(new Problem());
      sheets.verifyNoInteractions();
    }
  }
}
