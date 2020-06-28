package project;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static project.CLIUtil.runCLITool;

public class CLIComparisonTest {
    @TempDir
    Path cliGeneratedDir;
    Path schemaDir = Paths.get("src/main/avro");
    Path pluginGeneratedDir = Paths.get("build/generated-main-avro-java");

    @SuppressWarnings("unused")
    private static Stream<Arguments> compareSpecificCompilerOutput() {
        return Stream.of(
            // From https://stackoverflow.com/questions/45581437/how-to-specify-converter-for-default-value-in-avro-union-logical-type-fields
            Arguments.of("BuggyRecord.avsc", "com/example/BuggyRecord.java", "compile -dateTimeLogicalTypeImpl JODA schema".split(" ")),
            // From https://github.com/davidmc24/gradle-avro-plugin/issues/120
            Arguments.of("Messages.avsc", "com/somedomain/Messages.java", "compile -dateTimeLogicalTypeImpl JODA schema".split(" "))
        );
    }

    @ParameterizedTest
    @MethodSource
    void compareSpecificCompilerOutput(String schemaPath, String generatedPath, String... toolArgs) throws Exception {
        Path schemaFile = schemaDir.resolve(schemaPath);
        Path pluginGeneratedFile = pluginGeneratedDir.resolve(generatedPath);
        Path cliGeneratedFile = cliGeneratedDir.resolve(generatedPath);

        List<String> args = new ArrayList<>(Arrays.asList(toolArgs));
        args.add(schemaFile.toString());
        args.add(cliGeneratedDir.toString());
        runCLITool(args.toArray(new String[0]));
        
        String pluginGeneratedContent = readFile(pluginGeneratedFile);
        String cliGeneratedContent = readFile(cliGeneratedFile);
        Assertions.assertEquals(cliGeneratedContent, pluginGeneratedContent);
    }

    private static String readFile(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
}
