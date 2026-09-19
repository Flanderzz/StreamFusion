"""Exercise Maven's real assertion-failure, fork-crash and timeout result paths."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import summarize


class MavenResultIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agent = (Path(__file__).parent / "agent/target/streamfusion-flink-suite-agent-1.0-SNAPSHOT.jar").resolve()
        if not cls.agent.is_file():
            raise RuntimeError("Build the suite agent first: mvn -f dev/flink-suite/agent/pom.xml package")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        (cls.root / "pom.xml").write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion><groupId>test</groupId><artifactId>suite-status-probe</artifactId><version>1</version>
  <properties><maven.compiler.release>17</maven.compiler.release><project.build.sourceEncoding>UTF-8</project.build.sourceEncoding></properties>
  <dependencies><dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter</artifactId><version>5.10.2</version><scope>test</scope></dependency></dependencies>
  <build><plugins>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version></plugin>
    <plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-surefire-plugin</artifactId><version>3.2.2</version>
      <configuration><runOrder>alphabetical</runOrder><forkCount>1</forkCount><reuseForks>true</reuseForks>
        <forkedProcessTimeoutInSeconds>${probe.timeout.seconds}</forkedProcessTimeoutInSeconds>
      </configuration>
    </plugin>
  </plugins></build>
</project>''')
        source = cls.root / "src/test/java"
        source.mkdir(parents=True)
        (source / "AExpectedTest.java").write_text('''import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
class AExpectedTest {
  @Test void expected() {
    if (Boolean.getBoolean("probe.expected")) Assertions.fail("known upstream assertion");
  }
}''')
        (source / "ZAfterTest.java").write_text('''import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
class ZAfterTest {
  @Test void later() throws Exception {
    if (Boolean.getBoolean("probe.crash")) System.exit(27);
    if (Boolean.getBoolean("probe.timeout")) Thread.sleep(15000);
    if (Boolean.getBoolean("probe.unexpected")) Assertions.fail("unexpected assertion");
  }
}''')

    def test_real_maven_results_are_distinct_from_passing_or_allowed_xml(self):
        for scenario, flags, process_code, marker, summary_code in (
            ("passed", [], 0, "success", 0),
            ("allowed", ["expected"], 1, "test-failures", 0),
            ("unexpected", ["unexpected"], 1, "test-failures", 1),
            ("partial reports and fork crash", ["expected", "crash"], 1, "failure", 1),
            ("partial reports and timeout", ["expected", "timeout"], 1, "failure", 1),
        ):
            with self.subTest(scenario=scenario):
                reports = self.root / "target/surefire-reports"
                shutil.rmtree(reports, ignore_errors=True)
                evidence = self.root / "maven-result.tsv"
                evidence.unlink(missing_ok=True)
                command = ["mvn", "-B", "-ntp", "-f", str(self.root / "pom.xml"),
                           f"-Dmaven.ext.class.path={self.agent}",
                           f"-Dstreamfusion.flink-suite.maven-result={evidence}",
                           "-Dmaven.test.failure.ignore=false",
                           f"-Dprobe.timeout.seconds={2 if 'timeout' in flags else 60}",
                           *[f"-Dprobe.{flag}=true" for flag in flags], "test"]
                result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=90)
                self.assertEqual(process_code, result.returncode, result.stdout)
                self.assertTrue(evidence.exists(), result.stdout)
                self.assertEqual(f"streamfusion-maven-result-v1\t{marker}\n", evidence.read_text(), result.stdout)
                if "crash" in flags:
                    self.assertTrue((reports / "TEST-AExpectedTest.xml").is_file(), result.stdout)
                args = ["summarize.py", str(reports), "--process-exit", str(result.returncode),
                        "--maven-result", str(evidence), "--xfail", "AExpectedTest#expected"]
                with patch.object(sys, "argv", args), redirect_stdout(io.StringIO()):
                    self.assertEqual(summary_code, summarize.main(), result.stdout)


if __name__ == "__main__":
    unittest.main()
