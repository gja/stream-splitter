package in.gja.stream_splitter;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.lang.InterruptedException;

class StreamSplitterEmptyInputStream extends InputStream {
    public int read() {
        return -1;
    }

    public void close() { }
}

class InputPage {
    private byte[] buffer;
    private int length;

    InputPage(InputStream inputStream, int pageSize) throws IOException {
        buffer = new byte[pageSize];
        length = inputStream.read(buffer, 0, pageSize);
    }

    InputStream toInputStream() throws IOException {
        if (length == -1)
            return new StreamSplitterEmptyInputStream();
        else
            return new ByteArrayInputStream(buffer, 0, length);
    }
}

public class StreamSplitter {
    private Map<StreamSplitterStream, Integer> streams;
    private Map<Integer, InputPage> buffers;
    private int pageSize;
    private InputStream input;
    private int pageCount;

    public StreamSplitter(InputStream input, int pageSize, int pageCount) {
        streams = new HashMap<StreamSplitterStream, Integer>();
        buffers = new HashMap<Integer, InputPage>();
        this.pageSize = pageSize;
        this.input = input;
        this.pageCount = pageCount;
    }

    public StreamSplitter(InputStream input) {
        this(input, 8192, 4);
    }

    public static List<InputStream> splitStream(InputStream input, int number) {
        return splitStream(input, number, 8192, 4);
    }

    public static List<InputStream> splitStream(InputStream input, int number, int pageSize, int pageCount) {
        if(input == null)
            return new ArrayList<InputStream>();
        StreamSplitter splitter = new StreamSplitter(input, pageSize, pageCount);
        List<InputStream> list = new ArrayList<InputStream>();
        while(number-- > 0)
            list.add(splitter.newStream());
        return list;
    }

    public synchronized InputStream newStream() {
        StreamSplitterStream stream = new StreamSplitterStream(this);
        streams.put(stream, -1);
        return stream;
    }

    synchronized InputStream fetchPage(StreamSplitterStream stream) throws IOException {
        Integer pageNumber = streams.get(stream) + 1;
        streams.put(stream, pageNumber);

        InputPage page = buffers.get(pageNumber);
        if (page == null)
            page = populatePage(pageNumber);

        wakeUpSleepingThreads();

        return page.toInputStream();
    }

    private synchronized InputPage populatePage(Integer pageNumber) throws IOException {
        // Someone else has populated this while we waited
        if (buffers.get(pageNumber) != null)
            return buffers.get(pageNumber);

        deletePage(pageNumber - pageCount);

        InputPage page = new InputPage(input, pageSize);
        buffers.put(pageNumber, page);
        return page;
    }

    synchronized void deleteStream(InputStream inputStream) {
        streams.remove(inputStream);
        wakeUpSleepingThreads();
    }

    private synchronized void deletePage(Integer pageNumber) throws IOException {
        try {
            while(isPageInUse(pageNumber))
                wait();
        } catch (InterruptedException e) {
            throw new IOException("StreamSplitter: Interupted while waiting for inputStreams to catch up", e);
        }

        InputPage pageToDelete = buffers.get(pageNumber);
        if (pageToDelete == null)
            return;

        buffers.remove(pageNumber);
    }

    // Call from within synchronized(streams)
    private synchronized boolean isPageInUse(Integer pageNumber) {
        for (Integer n : streams.values())
            if (n == pageNumber)
                return true;
        return false;
    }

    private synchronized void wakeUpSleepingThreads() {
        notify();
    }
}


class StreamSplitterStream extends InputStream {
    private StreamSplitter parent;
    private InputStream inputStream;

    StreamSplitterStream(StreamSplitter parent) {
        this.parent = parent;
        this.inputStream = new StreamSplitterEmptyInputStream();
    }

    public int read() throws IOException {
        int ret = inputStream.read();

        if (ret != -1)
            return ret;

        inputStream.close();
        inputStream = parent.fetchPage(this);
        return inputStream.read();
    }

    public void close() throws IOException {
        inputStream.close();
        inputStream = new StreamSplitterEmptyInputStream();
        parent.deleteStream(this);
    }
}
