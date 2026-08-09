package za.co.fnb.dcre.agt;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Build-system and IDE roots live at the REPOSITORY ROOT and nowhere else.
 *
 * <p>Written after a whole second Gradle project appeared inside the Java package
 * directory {@code src/main/java/za/co/fnb/dcre/agt/}: its own {@code gradlew},
 * {@code gradlew.bat}, {@code gradle/wrapper/}, {@code .sdkmanrc}, a {@code .gradle}
 * cache proving Gradle had actually been RUN there, and an IntelliJ {@code .idea/}
 * plus {@code agt.iml} declaring that package directory a source root in its own
 * right. Four of those files reached the git INDEX.
 *
 * <p>The tell was unmistakable and is worth recording: the nested
 * {@code settings.gradle} was a byte-for-byte copy of the root one with the line
 * {@code package za.co.fnb.dcre.agt} prepended. Nothing legitimate produces a Gradle
 * settings file carrying a Java package declaration. It is the signature of a tool
 * run with its working directory set to a SOURCE directory instead of the project
 * root, which is the same class of defect as running a command in the wrong repo.
 *
 * <p>Why a test rather than a {@code .gitignore} entry: ignoring the paths only stops
 * them being COMMITTED. It does not stop them existing, and their mere existence is
 * the damage, because IntelliJ imports the nested {@code settings.gradle} as a second
 * project and every later search returns two of everything. This fails the BUILD, so
 * the defect cannot survive to a review. Same fail-closed reasoning as
 * {@code StageDatabases}: no default arm, no "everything else is fine".
 *
 * <p>Deliberately a plain JUnit test with no Quarkus context: it inspects the
 * checkout, not the application, and should cost nothing.
 */
class RepoLayoutTest {

    /** Files that mark a build root, a toolchain root, or an IDE module. */
    private static final Set<String> ROOT_ONLY_FILES = Set.of(
            "gradlew", "gradlew.bat",
            "settings.gradle", "settings.gradle.kts",
            "build.gradle", "build.gradle.kts",
            "gradle.properties",
            "gradle-wrapper.properties", "gradle-wrapper.jar",
            "pom.xml", "mvnw", "mvnw.cmd",
            ".sdkmanrc");

    /** Directories that mark an IDE or build root. */
    private static final Set<String> ROOT_ONLY_DIRS = Set.of(".idea", ".gradle", ".mvn");

    /** The module directory. Gradle sets user.dir to it for the test JVM. */
    private static final Path PROJECT_ROOT = Path.of("").toAbsolutePath();

    @Test
    void noBuildOrIdeRootExistsBelowSrc() throws IOException {
        final Path src = PROJECT_ROOT.resolve("src");
        // A missing src would make this test vacuously green, which is the failure
        // mode this whole file exists to refuse.
        assertTrue(Files.isDirectory(src), "src/ is missing at " + PROJECT_ROOT
                + ": this test cannot have verified anything.");

        final List<Path> offenders;
        try (Stream<Path> tree = Files.walk(src)) {
            offenders = tree.filter(RepoLayoutTest::isRootOnlyArtifact).sorted().toList();
        }

        assertTrue(offenders.isEmpty(), () -> """
                Build-system or IDE roots found inside the source tree.

                Build files belong at the repository root and nowhere else. A \
                settings.gradle, gradlew or .idea below src/ makes IntelliJ import a \
                SECOND, nested project, so the module is indexed twice and every search \
                returns duplicates.

                Cause to look for first: a command run with its working directory set to \
                a source directory instead of the project root (sdk env init, gradle \
                wrapper, a Quarkus CLI, or an editor creating a file on the package node).

                Fix: delete these, then re-run the command from %s.

                IF THE ONLY OFFENDER IS .idea AND IT COMES BACK AFTER YOU DELETE IT, deleting \
                is not the fix: a running IDE has that directory open as a PROJECT and rewrites \
                workspace.xml into it. Close that project, remove it from Recent Projects, and \
                open the repository root instead. Confirm which path it thinks is the project \
                with:
                  grep -o 'value="[^"]*<repo>[^"]*"' \
                    ~/Library/Application\\ Support/JetBrains/IntelliJIdea*/options/recentProjects.xml
                Do not delete this test to make the red go away; it is reporting a real, live \
                misconfiguration that has already put build files into git once.

                Offenders:
                  %s""".formatted(PROJECT_ROOT,
                String.join("\n  ", offenders.stream().map(PROJECT_ROOT::relativize)
                        .map(Path::toString).toList())));
    }

    /** A path under src/ that declares a build root, a toolchain root or an IDE module. */
    private static boolean isRootOnlyArtifact(final Path path) {
        final String name = path.getFileName().toString();
        if (Files.isDirectory(path)) {
            return ROOT_ONLY_DIRS.contains(name);
        }
        return ROOT_ONLY_FILES.contains(name) || name.endsWith(".iml");
    }
}
