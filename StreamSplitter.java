package in.gja.stream_splitter;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.CharBuffer;
import java.io.Reader;
import java.io.IOException;
import java.io.StringReader;
import java.io.CharArrayReader;

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

    Reader toReader() {
        if (length == -1)
            return new StreamSplitterEmptyReader();
        else
            return new CharArrayReader(buffer, 0, length);
    }
}

public class StreamSplitter {
    private Map<Reader, Integer> streamToBufferMap;
    private Map<Integer, InputBlock> buffers;
    private int blockSize;
    private Reader input;

    public StreamSplitter(Reader input, int blockSize) {
        streamToBufferMap = new ConcurrentHashMap<Reader, Integer>();
        buffers = new HashMap<Integer, InputBlock>();
        this.blockSize = blockSize;
        this.input = input;
    }

    public Reader getStream() {
        Reader buffer = new StreamSplitterStream(this);
        streamToBufferMap.put(buffer, -1);
        return buffer;
    }

    Reader nextBlock(Reader reader) throws IOException {
        Integer nextBlock = streamToBufferMap.get(reader) + 1;
        streamToBufferMap.put(reader, nextBlock);

        InputBlock block = buffers.get(nextBlock);
        if (block == null)
            block = populateBlock(nextBlock);
        return block.toReader();
    }

    void deleteStream(Reader reader) {
        streamToBufferMap.remove(reader);
    }

    private synchronized InputBlock populateBlock(Integer blockNumber) throws IOException {
        // Someone else has populated this while we waited
        if (buffers.get(blockNumber) != null)
            return buffers.get(blockNumber);

        InputBlock block = new InputBlock(input, blockSize);
        buffers.put(blockNumber, block);
        return block;
    }
}


class StreamSplitterStream extends Reader {
    private StreamSplitter parent;
    private Reader readable;

    StreamSplitterStream(StreamSplitter parent) {
        this.parent = parent;
        this.readable = new StreamSplitterEmptyReader();
    }

    public int read(char[] cbuf, int off, int len) throws IOException {
        int ret = readable.read(cbuf, off, len);

        if (ret != -1)
            return ret;

        readable.close();
        readable = parent.nextBlock(this);
        return readable.read(cbuf, off, len);
    }

    public void close() throws IOException {
        readable.close();
        readable = new StreamSplitterEmptyReader();
        parent.deleteStream(this);
    }
}
