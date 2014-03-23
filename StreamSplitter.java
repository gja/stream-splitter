package in.gja.stream_splitter;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.CharBuffer;
import java.io.Reader;
import java.io.IOException;
import java.io.StringReader;
import java.io.CharArrayReader;
import java.lang.InterruptedException;

class StreamSplitterEmptyReader extends Reader {
    public int read(char[] cbuf, int off, int len) {
        return -1;
    }

    public void close() { }
}

class InputBlock {
    private char[] buffer;
    private int length;

    InputBlock(Reader reader, int blockSize) throws IOException {
        buffer = new char[blockSize];
        length = reader.read(buffer, 0, blockSize);
    }

    Reader toReader() throws IOException {
        if (length == -1)
            return new StreamSplitterEmptyReader();
        else
            return new CharArrayReader(buffer, 0, length);
    }
}

public class StreamSplitter {
    private List<StreamSplitterStream> streams;
    private Map<Integer, InputBlock> buffers;
    private int blockSize;
    private Reader input;
    private int blockCount;

    public StreamSplitter(Reader input, int blockSize, int blockCount) {
        streams = new ArrayList<StreamSplitterStream>();
        buffers = new HashMap<Integer, InputBlock>();
        this.blockSize = blockSize;
        this.input = input;
        this.blockCount = blockCount;
    }

    public synchronized Reader getStream() {
        StreamSplitterStream buffer = new StreamSplitterStream(this);
        streams.add(buffer);
        return buffer;
    }

    Reader fetchBlock(int blockNumber) throws IOException {
        InputBlock block = buffers.get(blockNumber);
        if (block == null)
            block = populateBlock(blockNumber);

        wakeUpSleepingThreads();

        return block.toReader();
    }

    void deleteStream(Reader reader) {
        streams.remove(reader);
    }

    private synchronized void deleteBlock(Integer blockNumber) throws IOException {
        try {
            while(isBlockInUse(blockNumber))
                wait();

            InputBlock blockToDelete = buffers.get(blockNumber);
            if (blockToDelete == null)
                return;
            buffers.remove(blockNumber);
        } catch (InterruptedException e) {
            throw new IOException("StreamSplitter: Interupted while waiting for readers to catch up", e);
        }
    }

    private synchronized InputBlock populateBlock(Integer blockNumber) throws IOException {
        // Someone else has populated this while we waited
        if (buffers.get(blockNumber) != null)
            return buffers.get(blockNumber);

        // First delete the old block
        deleteBlock(blockNumber - blockCount);

        InputBlock block = new InputBlock(input, blockSize);
        buffers.put(blockNumber, block);
        return block;
    }

    private synchronized boolean isBlockInUse(int blockNumber) {
        for (StreamSplitterStream stream : streams)
            if (stream.blockNumber == blockNumber)
                return true;

        return false;
    }

    private synchronized void wakeUpSleepingThreads() {
        notify();
    }
}



class StreamSplitterStream extends Reader {
    private StreamSplitter parent;
    private Reader readable;
    int blockNumber;

    StreamSplitterStream(StreamSplitter parent) {
        this.parent = parent;
        this.readable = new StreamSplitterEmptyReader();
        blockNumber = -1;
    }

    public synchronized int read(char[] cbuf, int off, int len) throws IOException {
        int ret = readable.read(cbuf, off, len);

        if (ret != -1)
            return ret;

        readable.close();
        readable = parent.fetchBlock(++blockNumber);
        return readable.read(cbuf, off, len);
    }

    public synchronized void close() throws IOException {
        readable.close();
        readable = new StreamSplitterEmptyReader();
        parent.deleteStream(this);
    }
}
