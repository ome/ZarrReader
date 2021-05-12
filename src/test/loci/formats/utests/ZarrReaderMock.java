package test.loci.formats.utests;

import java.io.IOException;

import loci.formats.FormatException;
import loci.formats.in.ZarrReader;
import loci.formats.services.ZarrService;

public class ZarrReaderMock extends ZarrReader {

  private ZarrService mockService;

  public ZarrReaderMock(ZarrService zarrService) {
    mockService = zarrService;
  }

  @Override
  protected void initializeZarrService(String rootPath) throws IOException, FormatException {
    zarrService = mockService;
  }
}
