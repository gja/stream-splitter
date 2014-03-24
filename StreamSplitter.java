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
    private List<StreamSplitterStream> streams;
    private Map<Integer, InputPage> buffers;
    private int pageSize;
    private InputStream input;
    private int pageCount;

    public StreamSplitter(InputStream input, int pageSize, int pageCount) {
        streams = new ArrayList<StreamSplitterStream>();
        buffers = new HashMap<Integer, InputPage>();
        this.pageSize = pageSize;
        this.input = input;
        this.pageCount = pageCount;
    }

    public StreamSplitter(InputStream input) {
        this(input, 8192, 4);
    }

    public InputStream newStream() {
        StreamSplitterStream buffer = new StreamSplitterStream(this);
        synchronized(streams) {
            streams.add(buffer);
        }
        return buffer;
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

    InputStream fetchPage(int pageNumber) throws IOException {
        InputPage page = buffers.get(pageNumber);
        if (page == null)
            page = populatePage(pageNumber);

        wakeUpSleepingThreads();

        return page.toInputStream();
    }

    void deleteStream(InputStream inputStream) {
        synchronized(streams) {
            streams.remove(inputStream);
            wakeUpSleepingThreads();
        }
    }

    private void deletePage(Integer pageNumber) throws IOException {
        synchronized(streams) {
            try {
                while(isPageInUse(pageNumber))
                    streams.wait();
            } catch (InterruptedException e) {
                throw new IOException("StreamSplitter: Interupted while waiting for inputStreams to catch up", e);
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


class StreamSplitterStream extends InputStream {
    private StreamSplitter parent;
    private InputStream inputStream;
    int pageNumber;

    StreamSplitterStream(StreamSplitter parent) {
        this.parent = parent;
        this.inputStream = new StreamSplitterEmptyInputStream();
        pageNumber = -1;
    }

    public int read() throws IOException {
        int ret = inputStream.read();

        if (ret != -1)
            return ret;

        inputStream.close();
        inputStream = parent.fetchPage(++pageNumber);
        return inputStream.read();
    }

    public void close() throws IOException {
        inputStream.close();
        inputStream = new StreamSplitterEmptyInputStream();
        parent.deleteStream(this);
    }
}
