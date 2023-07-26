package datastream.sources.file.impl;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class FileSource implements Source<String, FileSplit, FileCheckpoint> {
    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SplitEnumerator<FileSplit, FileCheckpoint> createEnumerator(SplitEnumeratorContext<FileSplit> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<FileSplit, FileCheckpoint> restoreEnumerator(SplitEnumeratorContext<FileSplit> enumContext, FileCheckpoint checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<FileSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<FileCheckpoint> getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public SourceReader<String, FileSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

}
