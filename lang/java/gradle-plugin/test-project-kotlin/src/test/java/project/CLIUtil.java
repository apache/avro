package project;

import org.apache.avro.tool.Main;
import org.junit.jupiter.api.Assertions;

class CLIUtil {
    private static final int STATUS_SUCCESS = 0;

    static void runCLITool(String... args) throws Exception {
        SystemUtil.forbidSystemExitCall();
        try {
            Main.main(args);
        } catch (SystemUtil.ExitTrappedException ex) {
            Assertions.assertEquals(STATUS_SUCCESS, ex.getStatus(), "CLI tool failed");
        } finally {
            SystemUtil.allowSystemExitCall();
        }
    }
}
