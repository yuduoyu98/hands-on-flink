package datastream.sources.file.impl;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class FileReader implements SourceReader<String, FileSplit> {
    @Override
    public void start() {

    }

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) throws Exception {
        return null;
    }

    @Override
    public List<FileSplit> snapshotState(long checkpointId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<FileSplit> splits) {

    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {

    }
}
