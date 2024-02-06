package xyz.liangck.jitcask;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author: liangck
 * @since: 2019/1/3 19:52
 * @description:
 */
public class KeyDir {

    private final Map<String, Entry> map = new ConcurrentHashMap<>();

    public Entry get(byte[] key) {
        return map.get(encodeKey(key));
    }

    public boolean put(byte[] key, Entry entry) {
        map.put(encodeKey(key), entry);
        return true;
    }

    public static String encodeKey(byte[] key) {
        return Base64.getEncoder().encodeToString(key);
    }

    public void delete(byte[] key) {
        map.remove(encodeKey(key));
    }

    public List<byte[]> listKeys() {
        if (map.isEmpty()) {
            return Collections.emptyList();
        }

        return map.values().stream().map(Entry::getKey).collect(Collectors.toList());
    }

    public boolean hasKey(byte[] key) {
        return map.containsKey(encodeKey(key));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Entry {
        private byte[] key;

        private int fileId;

        private int totalSize;

        private long offset;

        private int tstamp;
    }
}
