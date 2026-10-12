// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.nio.charset.StandardCharsets;
import pro.surpin.pgpaimon.writer.dto.*;

/** The DDL message credential as unit tests spell it; see {@link DdlMessageHandler#handle}. */
final class TestAuth {

  static final String TOKEN = "test-token";

  private TestAuth() {}

  /** {@code json} (a JSON object) carrying the credential, as paimon.emit adds it. */
  static byte[] authed(
      String json
  ) {
    int brace = json.indexOf('{');
    String withAuth = brace < 0 ? json
        : json.substring(0, brace + 1) + "\"auth\":\"" + TOKEN + "\"," + json.substring(brace + 1);
    return withAuth.getBytes(StandardCharsets.UTF_8);
  }
}
