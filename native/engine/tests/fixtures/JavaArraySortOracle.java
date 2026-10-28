import java.util.Arrays;
import java.util.Random;

/** Regenerate with the supported JDK: java JavaArraySortOracle.java > java-sort-oracle.json. */
public final class JavaArraySortOracle {
  public static void main(String[] args) {
    Random random = new Random(505);
    StringBuilder json = new StringBuilder("[");
    boolean first = true;
    for (int size : new int[] {0, 1, 2, 8, 16, 31, 32, 33, 63, 64, 65, 128, 257, 512, 1024}) {
      for (int sample = 0; sample < 8; sample++) {
        for (boolean ascending : new boolean[] {true, false}) {
          for (boolean nullFirst : new boolean[] {true, false}) {
            Integer[] keys = new Integer[size];
            Integer[] indices = new Integer[size];
            for (int i = 0; i < size; i++) {
              keys[i] = random.nextInt(11) == 0 ? null : random.nextInt(sample + 2);
              indices[i] = i;
            }
            boolean cycle = sample == 7;
            boolean failed = false;
            try {
              Arrays.sort(indices, (left, right) -> {
                Integer a = keys[left];
                Integer b = keys[right];
                if (a == null || b == null) {
                  return a == b ? 0 : (a == null ? -1 : 1) * (nullFirst ? 1 : -1);
                }
                boolean greater = cycle ? Math.floorMod(a - b, 3) == 1 : a > b;
                return (greater ? 1 : -1) * (ascending ? 1 : -1);
              });
            } catch (IllegalArgumentException expected) {
              if (!expected.getMessage().equals("Comparison method violates its general contract!")) {
                throw expected;
              }
              failed = true;
            }
            if (!first) json.append(',');
            first = false;
            json.append("{\"keys\":").append(Arrays.toString(keys))
                .append(",\"ascending\":").append(ascending)
                .append(",\"nullFirst\":").append(nullFirst)
                .append(",\"cycle\":").append(cycle)
                .append(",\"output\":").append(failed ? "null" : Arrays.toString(indices))
                .append('}');
          }
        }
      }
    }
    System.out.println(json.append(']'));
  }
}
