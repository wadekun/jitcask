package xyz.liangck.jitcask;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

/**
 * @author: liangck
 * @since: 2019/1/3 18:20
 * @description: 代表一个bitcask的数据文件
 */
public class DataFile {
    public static final String suffix = ".bitcask.data";

    public static final String tombstone = "bitcask_tombstone";

    // 4 + 4 + 4 + 4
    public static final int HEADER_SIZE = 16;

    public static final Pattern filenamePattern = Pattern.compile("^\\d{10}\\.bitcask\\.data$");


    /**
     * file id
     */
    private int id;

    private final File originFile;

    private FileChannel wch;
    private FileChannel rch;

    private AtomicLong writeOffset;

    public int getId() {
        return id;
    }

    public DataFile(int id, File originFile, FileChannel wch, FileChannel rch) throws IOException {
        this.id = id;
        this.originFile = originFile;
        this.wch = wch;
        this.rch = rch;
        writeOffset = new AtomicLong(wch.size());
    }

    public static DataFile open(File dataDir, int id) throws IOException {
        String fileName = mkFileName(id);
        File dataFile = new File(dataDir, fileName);
        return open(dataFile);
    }

    public static DataFile open(File file) throws IOException {
        int id = parseId(file);
        FileOutputStream fos = new FileOutputStream(file, true);
        RandomAccessFile rf = new RandomAccessFile(file, "r");
        return new DataFile(id, file, fos.getChannel(), rf.getChannel());
    }

    public static int parseId(File file) {
        if (!file.exists()) {
            throw new IllegalArgumentException("file " + file.getName() + " not found");
        }
        Matcher matcher = filenamePattern.matcher(file.getName());
        if (matcher.matches()) {
            int indexOf = file.getName().indexOf(".");
            return Integer.parseInt(file.getName().substring(0, indexOf));
        }

        throw new IllegalArgumentException("file " + file.getName() + " is not valid");
    }

    static DataFile create(File dataDir) throws IOException {
        int tstamp = tstamp();

        File newFile = new File(dataDir, mkFileName(tstamp));
        boolean created = false;
        while (!created) {
            created = newFile.createNewFile();
            if (!created) {
                tstamp++;
                newFile = new File(dataDir, mkFileName(tstamp));
            }
        }

        RandomAccessFile wf = new RandomAccessFile(newFile, "rw");
        RandomAccessFile rf = new RandomAccessFile(newFile, "r");
        return new DataFile(tstamp, newFile, wf.getChannel(), rf.getChannel());
    }

    private static String mkFileName(long tstamp) {
        return tstamp + suffix;
    }

    private static int tstamp() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public List<Entry> readAllEntries() {
        // TODO: 2019/1/6 read all entries

        return Collections.emptyList();
    }

    public void sync() throws IOException {
        wch.force(true);
    }

    public void delete(byte[] key) throws IOException {
        this.write(key, tombstone.getBytes(StandardCharsets.UTF_8));
    }

    static class Entry {

        private long position;

        private int crc;

        private int tstamp;

        private int ksz;

        private int valueSize;

        private byte[] key;

        private byte[] value;

        public Entry() {
        }

        public Entry(long position, int crc, int tstamp, int ksz, int valueSize, byte[] key, byte[] value) {
            this.position = position;
            this.crc = crc;
            this.tstamp = tstamp;
            this.ksz = ksz;
            this.valueSize = valueSize;
            this.key = key;
            this.value = value;
        }

        public boolean isDeleted() {
            return Objects.equals(new String(value, StandardCharsets.UTF_8), tombstone);
        }

        public long getPosition() {
            return position;
        }

        public int getCrc() {
            return crc;
        }

        public int getTstamp() {
            return tstamp;
        }

        public int getKsz() {
            return ksz;
        }

        public int getValueSize() {
            return valueSize;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }

    public <T> T foldKeys(Function<Byte[], T> func, T acc) {
        // TODO: 2019/1/6 fold all keys in this file


        return null;
    }

    public <T> T fold(BiFunction<Entry, T, T> func, T acc) throws IOException {
        long offset = 0;

        while (offset < originFile.length()
                && (offset + HEADER_SIZE) < originFile.length()) {

            byte[] header = new byte[HEADER_SIZE];
            ByteBuffer headerBuf = ByteBuffer.wrap(header);
            this.readBuf(headerBuf, offset, HEADER_SIZE);

            int crc = headerBuf.getInt(0);
            int tstamp = headerBuf.getInt(4);
            int ksz = headerBuf.getInt(8);
            int valueSz = headerBuf.getInt(12);

            long keyOffset = offset + HEADER_SIZE;
            byte[] key = new byte[ksz];
            ByteBuffer keyBuf = ByteBuffer.wrap(key);
            this.readBuf(keyBuf, keyOffset, ksz);

            long valueOffset = keyOffset + ksz;
            byte[] value = new byte[valueSz];
            ByteBuffer valueBuf = ByteBuffer.wrap(value);
            this.readBuf(valueBuf, valueOffset, valueSz);

            Entry entry = new Entry(offset, crc, tstamp, ksz, valueSz, key, value);

            acc = func.apply(entry, acc);

            offset = valueOffset + valueSz;
        }

        return acc;
    }

    private void readBuf(ByteBuffer headerBuf, long offset, int size) throws IOException {
        int read = 0;
        do {
            read += rch.read(headerBuf, offset);
        } while (read < size);
    }

    public KeyDir.Entry write(byte[] key, byte[] value) throws IOException {
        int tstamp = tstamp();
        int key_sz = key.length;
        int value_sz = value.length;

        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer headerBuf = ByteBuffer.wrap(header);
        headerBuf.putInt(4, tstamp);
        headerBuf.putInt(8, key_sz);
        headerBuf.putInt(12, value_sz);

        ByteBuffer keyBuf = ByteBuffer.wrap(key);
        ByteBuffer valueBuf = ByteBuffer.wrap(value);

        CRC32 crc32 = new CRC32();
        crc32.update(header, 4, 12);
        crc32.update(keyBuf);
        crc32.update(valueBuf);
        long crc = crc32.getValue();
        headerBuf.putInt(0, (int) crc);
        ByteBuffer[] buffers = {headerBuf, keyBuf, valueBuf};
        rewind(buffers);
        long left = length(buffers);
        while (left > 0) {
            left -= wch.write(buffers);
        }

        int entrySize = HEADER_SIZE + key_sz + value_sz;
        long offset = writeOffset.getAndAdd(entrySize);

        return new KeyDir.Entry(key, this.id, entrySize, offset, tstamp);
    }

    private static void rewind(ByteBuffer[] buffers) {
        for (ByteBuffer buffer : buffers) {
            buffer.rewind();
        }
    }

    private static long length(ByteBuffer[] buffers) {
        long len = 0;
        for (ByteBuffer buffer : buffers) {
            len += buffer.remaining();
        }
        return len;
    }

    public byte[] read(long offset, int entrySize) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer headerBuf = ByteBuffer.wrap(header);
        int read = 0;
        do {
            read += rch.read(headerBuf, offset);
        } while (read < HEADER_SIZE);

        int keySz = headerBuf.getInt(8);
        int valueSz = headerBuf.getInt(12);

        if (entrySize != (HEADER_SIZE + keySz + valueSz)) {
            throw new IllegalArgumentException("Bad entry size");
        }

        byte[] kv = new byte[keySz + valueSz];
        ByteBuffer kvBuf = ByteBuffer.wrap(kv);
        int readValueBytes = 0;
        do {
            readValueBytes += rch.read(kvBuf, offset + HEADER_SIZE);
        } while (readValueBytes < (keySz + valueSz));

        CRC32 crc32 = new CRC32();
        crc32.update(header, 4, 12);
        crc32.update(kv, 0, keySz);
        crc32.update(kv, keySz, valueSz);
        long crc = crc32.getValue();
        if (((int) crc) != headerBuf.getInt(0)) {
            throw new IOException("Crc verification failed");
        }
        return Arrays.copyOfRange(kv, keySz, kv.length);
    }

    public void close() throws IOException {
        if (Objects.nonNull(wch)) {
            wch.close();
        }

        if (Objects.nonNull(rch)) {
            rch.close();
        }
    }

    public boolean closeAndDelete() throws IOException {
        close();
        return originFile.delete();
    }
}
