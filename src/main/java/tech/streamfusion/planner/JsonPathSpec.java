package tech.streamfusion.planner;

import java.util.Locale;
import java.util.regex.Pattern;

/** Admission grammar shared with the native SQL/JSON definite-path reader. */
final class JsonPathSpec {
  private static final Pattern MODE =
      Pattern.compile("^\\s*(strict|lax)\\s+(.+)$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern STEP =
      Pattern.compile("\\.[A-Za-z_][A-Za-z0-9_]*|\\['[A-Za-z0-9_ -]+'\\]|\\[([0-9]+)\\]");

  private JsonPathSpec() {}

  static String unicodeVersion() {
    // Jackson terminates root true/false/null tokens with Character.isJavaIdentifierPart(char).
    return switch (Runtime.version().feature()) {
      case 17 -> "13.0";
      case 21 -> "15.0";
      case 24, 25 -> "16.0";
      default -> null;
    };
  }

  static String normalize(String path) {
    if (unicodeVersion() == null) {
      return null;
    }
    var mode = MODE.matcher(path);
    String prefix = "strict ";
    if (mode.matches()) {
      prefix = mode.group(1).toLowerCase(Locale.ROOT) + " ";
      path = mode.group(2);
    }
    if (!path.startsWith("$")) {
      return null;
    }
    var step = STEP.matcher(path);
    int end = 1;
    while (end < path.length()) {
      step.region(end, path.length());
      if (!step.lookingAt()) {
        return null;
      }
      if (step.group(1) != null) {
        try {
          Integer.parseInt(step.group(1));
        } catch (NumberFormatException e) {
          return null;
        }
      }
      end = step.end();
    }
    return prefix + path;
  }
}
