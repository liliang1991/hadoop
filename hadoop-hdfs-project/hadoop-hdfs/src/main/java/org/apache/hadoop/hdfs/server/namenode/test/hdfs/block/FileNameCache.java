package org.apache.hadoop.hdfs.server.namenode.test.hdfs.block;


import java.util.HashMap;
import java.util.Map;

public class FileNameCache<K> {
    private int lookups = 0;

    private class UseCount {
        int count;
        final K value;  // Internal value for the name

        UseCount(final K value) {
            count = 1;
            this.value = value;
        }

        void increment() {
            count++;
        }

        int get() {
            return count;
        }
    }

    int useThreshold;
    public boolean initialized = false;
    Map<K, UseCount> transientMap = new HashMap<K, UseCount>();

    final HashMap<K, K> cache = new HashMap<K, K>();

    public FileNameCache(int useThreshold) {
        this.useThreshold = useThreshold;
    }

    public void reset() {
        initialized = false;
        cache.clear();
        if (transientMap == null) {
            transientMap = new HashMap<K, UseCount>();
        } else {
            transientMap.clear();
        }


    }

    public K put(final K name) {
        K internal = cache.get(name);
        if (internal != null) {
            lookups++;
            return internal;
        }

        // Track the usage count only during initialization
        if (!initialized) {
            UseCount useCount = transientMap.get(name);
            if (useCount != null) {
                useCount.increment();
                if (useCount.get() >= useThreshold) {
                    promote(name);
                }
                return useCount.value;
            }
            useCount = new UseCount(name);
            transientMap.put(name, useCount);
        }
        return null;
    }

    private void promote(final K name) {
        transientMap.remove(name);
        cache.put(name, name);
        lookups += useThreshold;
    }
  public   void initialized() {
        this.initialized = true;
        transientMap.clear();
        transientMap = null;
    }
}


