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

class InputPage {
    private char[] buffer;
    private int length;

    InputPage(Reader reader, int pageSize) throws IOException {
        buffer = new char[pageSize];
        length = reader.read(buffer, 0, pageSize);
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
    private Map<Integer, InputPage> buffers;
    private int pageSize;
    private Reader input;
    private int pageCount;

    public StreamSplitter(Reader input, int pageSize, int pageCount) {
        streams = new ArrayList<StreamSplitterStream>();
        buffers = new HashMap<Integer, InputPage>();
        this.pageSize = pageSize;
        this.input = input;
        this.pageCount = pageCount;
    }

    public StreamSplitter(Reader input) {
        this(input, 262144, 4);
    }

    public Reader newStream() {
        StreamSplitterStream buffer = new StreamSplitterStream(this);
        synchronized(streams) {
            streams.add(buffer);
        }
        return buffer;
    }

    public static List<Reader> splitStream(Reader input, int number) {
        return splitStream(input, number, 262144, 4);
    }

    public static List<Reader> splitStream(Reader input, int number, int pageSize, int pageCount) {
        StreamSplitter splitter = new StreamSplitter(input, pageSize, pageCount);
        List<Reader> list = new ArrayList<Reader>();
        while(number-- > 0)
            list.add(splitter.newStream());
        return list;
    }

    Reader fetchPage(int pageNumber) throws IOException {
        InputPage page = buffers.get(pageNumber);
        if (page == null)
            page = populatePage(pageNumber);

        wakeUpSleepingThreads();

        return page.toReader();
    }

    void deleteStream(Reader reader) {
        synchronized(streams) {
            streams.remove(reader);
            wakeUpSleepingThreads();
        }
    }

    private void deletePage(Integer pageNumber) throws IOException {
        synchronized(streams) {
            try {
                while(isPageInUse(pageNumber))
                    streams.wait();
            } catch (InterruptedException e) {
                throw new IOException("StreamSplitter: Interupted while waiting for readers to catch up", e);
            }
        }


        InputPage pageToDelete = buffers.get(pageNumber);
        if (pageToDelete == null)
            return;

        synchronized(buffers) {
            buffers.remove(pageNumber);
        }
    }

    private InputPage populatePage(Integer pageNumber) throws IOException {
        synchronized(buffers) {
            // Someone else has populated this while we waited
            if (buffers.get(pageNumber) != null)
                return buffers.get(pageNumber);

            deletePage(pageNumber - pageCount);

            InputPage page = new InputPage(input, pageSize);
            buffers.put(pageNumber, page);
            return page;
        }
    }

    // Call from within synchronized(streams)
    private boolean isPageInUse(int pageNumber) {
        for (StreamSplitterStream stream : streams)
            if (stream.pageNumber == pageNumber)
                return true;
        return false;
    }

    private void wakeUpSleepingThreads() {
        synchronized(streams) {
            streams.notify();
        }
    }
}


class StreamSplitterStream extends Reader {
    private StreamSplitter parent;
    private Reader reader;
    int pageNumber;

    StreamSplitterStream(StreamSplitter parent) {
        this.parent = parent;
        this.reader = new StreamSplitterEmptyReader();
        pageNumber = -1;
    }

    public synchronized int read(char[] cbuf, int off, int len) throws IOException {
        int ret = reader.read(cbuf, off, len);

        if (ret != -1)
            return ret;

        reader.close();
        reader = parent.fetchPage(++pageNumber);
        return reader.read(cbuf, off, len);
    }

    public synchronized void close() throws IOException {
        reader.close();
        reader = new StreamSplitterEmptyReader();
        parent.deleteStream(this);
    }
}
