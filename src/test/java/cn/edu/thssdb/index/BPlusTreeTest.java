package cn.edu.thssdb.index;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BPlusTreeTest {
  private BPlusTree<Integer, Integer> tree;
  private ArrayList<Integer> keys;
  private ArrayList<Integer> values;
  private HashMap<Integer, Integer> map;

  @Before
  public void setUp() {
    tree = new BPlusTree<>();
    keys = new ArrayList<>();
    values = new ArrayList<>();
    map = new HashMap<>();
    HashSet<Integer> set = new HashSet<>();
    int size = 10000;
    for (int i = 0; i < size; i++) {
      double random = Math.random();
      set.add((int) (random * size));
    }
    for (Integer key : set) {
      int hashCode = key.hashCode();
      keys.add(key);
      values.add(hashCode);
      tree.put(key, hashCode);
      map.put(key, hashCode);
    }
  }

  @Test
  public void testGet() {
    for (Integer key : keys) {
      // check if all the results equal
      assertEquals(map.get(key), tree.get(key));
    }
  }

  @Test
  public void testRemove() {
    int size = keys.size();
    for (int i = 0; i < size; i += 2) {
      // remove half data
      tree.remove(keys.get(i));
    }
    // check if size equals half of origin
    assertEquals(size / 2, tree.size());
    for (int i = 1; i < size; i += 2) {
      assertEquals(map.get(keys.get(i)), tree.get(keys.get(i)));
    }
  }

  @Test
  public void testIterator() {
    BPlusTreeIterator<Integer, Integer> iterator = tree.iterator();
    int c = 0;
    while (iterator.hasNext()) {
      assertTrue(values.contains(iterator.next().right));
      c++;
    }
    assertEquals(values.size(), c);
  }
}
