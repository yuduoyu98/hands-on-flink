package datastream.sources.file.impl;

import datastream.sources.file.impl.FileCheckpoint;
import datastream.sources.file.impl.FileSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class FileSplitEnumerator implements SplitEnumerator<FileSplit, FileCheckpoint> {
    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List<FileSplit> splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public FileCheckpoint snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
