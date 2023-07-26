package datastream.sources.file.impl;

import org.apache.flink.api.connector.source.SourceSplit;

public class FileSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
