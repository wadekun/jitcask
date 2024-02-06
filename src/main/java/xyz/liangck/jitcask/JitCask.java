package xyz.liangck.jitcask;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Paper link: https://riak.com/assets/bitcask-intro.pdf
 * @since: 2019/1/3 18:20
 * @author: liangck
 */
public class JitCask {

    KeyDir keyDir;

    File dataDir;

    DataFile activeDataFile;

    Map<Integer, DataFile> readFiles = new ConcurrentHashMap<>();

    /**
     * Open a new or existing Bitcask datastore with additional options.
     * Valid options include read write (if this process is going to be a
     * writer and not just a reader) and sync on put (if this writer would
     * prefer to sync the write file after every write operation).
     * The directory must be readable and writable by this process, and
     * only one process may open a Bitcask with read write at a time.
     *
     * bitcask:open(DirectoryName, Opts)
     * → BitCaskHandle | {error, any()}
     */
    public static JitCask open(String dir, Operations opts) throws IOException {
        Path dirPath = Path.of(dir);
        if (Files.notExists(dirPath)) {
            throw new RuntimeException("data dir: " + dir + " not exists!");
        }

        JitCask jitCask = new JitCask();
        jitCask.keyDir = new KeyDir();
        jitCask.dataDir = new File(dir);
        jitCask.scanAllFiles();
//        jitCask.merge();

        return jitCask;
    }

    private void scanAllFiles() throws IOException {
        File[] files = findAllDataFiles();
        if (files == null) return;

        for (File file : files) {
            DataFile dataFile = DataFile.open(file);
            dataFile.fold((BiFunction<DataFile.Entry, Void, Void>) (entry, unused) -> {
                if (entry.isDeleted()) {
                    keyDir.delete(entry.getKey());
                } else {
                    int totalSize = DataFile.HEADER_SIZE + entry.getKsz() + entry.getValueSize();
                    keyDir.put(entry.getKey(), new KeyDir.Entry(entry.getKey(), dataFile.getId(), totalSize, entry.getPosition(), entry.getTstamp()));
                }
                return null;
            }, null);
        }
    }

    private File[] findAllDataFiles() {
        File[] files = this.dataDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return DataFile.filenamePattern.matcher(pathname.getName()).matches();
            }
        });

        if (Objects.isNull(files)) {
            return null;
        }

        // file id 正序排序
        Arrays.sort(files, Comparator.comparingInt(DataFile::parseId));
        return files;
    }

    /**
     * Open a new or existing Bitcask datastore for read-only access.
     * The directory and all files in it must be readable by this process.
     *
     * bitcask:open(DirectoryName)
     * → BitCaskHandle | {error, any()}
     */
    public static JitCask open(String dir) throws IOException {
        
        return open(dir, Operations.read_only);
    }
    
    /**
     * Retrieve a value by key from a Bitcask datastore.
     *
     * bitcask:get(BitCaskHandle, Key)
     * → not found | {ok, Value}
     */
    public byte[] get(byte[] key) throws IOException {
        KeyDir.Entry entry = keyDir.get(key);
        if (Objects.isNull(entry)) {
            return null;
        }

        DataFile dataFile = readFiles.get(entry.getFileId());
        if (Objects.isNull(dataFile)) {
            dataFile = DataFile.open(dataDir, entry.getFileId());
        }

        return dataFile.read(entry.getOffset(), entry.getTotalSize());
    }
    
    /**
     * Store a key and value in a Bitcask datastore.
     *
     * bitcask:put(BitCaskHandle, Key, Value)
     * → ok | {error, any()}
     */
    public Boolean put(byte[] key, byte[] value) throws IOException {
        ensureActiveDataFile();

        KeyDir.Entry entry = activeDataFile.write(key, value);
        keyDir.put(key, entry);

        return Boolean.FALSE;
    }

    public Boolean put(String key, String value) throws IOException {
        return put(key.getBytes(StandardCharsets.UTF_8), String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }

    public String get(String key) throws IOException {
        return new String(get(key.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    private void ensureActiveDataFile() throws IOException {
        if (Objects.isNull(this.activeDataFile)) {
            this.activeDataFile = DataFile.create(this.dataDir);
        }
    }

    /**
     * Delete a key from a Bitcask datastore.
     *
     * bitcask:delete(BitCaskHandle, Key)
     * → ok | {error, any()}
     */
    public Boolean delete(byte[] key) throws IOException {
        // TODO: 2019/1/5
        ensureActiveDataFile();
        keyDir.delete(key);
        this.activeDataFile.delete(key);
        return Boolean.FALSE;
    }
    
    /**
     * List all keys in a Bitcask datastore.
     *
     * bitcask:list keys(BitCaskHandle)
     * → [Key] | {error, any()}
     */
    public List<byte[]> keys() {
        // TODO: 2019/1/5  
        return keyDir.listKeys();
    }
    
    /**
     * Fold over all K/V pairs in a Bitcask datastore.
     * Fun is expected to be of the form: F(K,V,Acc0) → Acc.
     *
     * bitcask:fold(BitCaskHandle,Fun,Acc0)
     * → Acc
     */
    public <R> R fold(BiFunction<R, byte[], R> func) {
        // TODO: 2019/1/5  
        
        return null;
    }
    
    /**
     * Merge several data files within a Bitcask datastore into a more compact form.
     * Also, produce hintfiles for faster startup.
     *
     * bitcask:merge(DirectoryName)
     * → ok | {error, any()}
     */
    public Boolean merge() throws IOException {
        if (Objects.isNull(dataDir)) {
            throw new IllegalStateException("dataDir is null");
        }
        File[] files = findAllDataFiles();
        if (files == null) return Boolean.TRUE;

        // max file size: 500MB
        long maxFileSize = 1000 * 1024 * 500;
        Map<String, DataFile.Entry> mergedEntries = new HashMap<>();
        List<DataFile> mergedFiles = new ArrayList<>();

        // TODO: produce hintfiles
        for (File file : files) {
            DataFile dataFile = DataFile.open(file);
            dataFile.fold((BiFunction<DataFile.Entry, Void, Void>) (entry, unused) -> {
                if (keyDir.hasKey(entry.getKey())) {
                    mergedEntries.put(KeyDir.encodeKey(entry.getKey()), entry);
                }

                return null;
            }, null);

            mergedFiles.add(dataFile);
            long mergedTotalSize = getSize(mergedEntries);
            if (mergedTotalSize >= maxFileSize) {
                mergeAndClear(mergedEntries, mergedFiles, true);
            }
        }

        mergeAndClear(mergedEntries, mergedFiles, false);

        return Boolean.TRUE;
    }

    private void mergeAndClear(Map<String, DataFile.Entry> mergedEntries, List<DataFile> mergedFiles, boolean closeNewFile) throws IOException {
        DataFile newFile = DataFile.create(dataDir);
        for (DataFile.Entry mergedEntry : mergedEntries.values()) {
            KeyDir.Entry writedEntry = newFile.write(mergedEntry.getKey(), mergedEntry.getValue());
            keyDir.put(mergedEntry.getKey(), writedEntry);
        }
        mergedEntries.clear();
        for (DataFile mergedFile : mergedFiles) {
            mergedFile.closeAndDelete();
        }
        mergedFiles.clear();
        if (!closeNewFile) {
            this.activeDataFile = newFile;
        }
    }

    private long getSize(Map<String, DataFile.Entry> mergedEntries) {
        if (Objects.isNull(mergedEntries) || mergedEntries.size() == 0) {
            return 0;
        }

        return mergedEntries.values().stream().mapToLong(entry -> DataFile.HEADER_SIZE + entry.getKsz() + entry.getValueSize()).sum();
    }

    /**
     * bitcask:sync(BitCaskHandle) Force any writes to sync to disk.
     * → ok
     */
    public Boolean sync() throws IOException {
        // TODO: 2019/1/5

        activeDataFile.sync();

        return Boolean.TRUE;
    }


    /**
     * Close a Bitcask data store and flush all pending writes to disk.
     *
     * bitcask:close(BitCaskHandle)
     * → ok (if any)
     */
    public Boolean close() throws IOException {
        if (Objects.nonNull(readFiles) && readFiles.size() > 0) {
            for (Map.Entry<Integer, DataFile> fileEntry : readFiles.entrySet()) {
                fileEntry.getValue().close();
            }
        }

        readFiles.clear();

        // jitCask应该也要加个状态
        // setState(closed);

        return Boolean.TRUE;
    }
    
}
