import java.util.*;

/** Reproduce the map-cast tree-bin fixture with JDK 17: java JavaMapCastOracle.java. */
class JavaMapCastOracle {
  public static void main(String[] args) {
    Map<Long, Integer> map = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      long bits = ((long) i << 32) | i;
      map.put(i % 2 == 0 ? bits : ~bits, i);
    }
    map.put(null, 100);
    System.out.println(map.values());
  }
}
