package org.query.calc.persistence;

import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Slf4j
public class TableWritter {
    private static final Character NEW_LINE = '\n';
    private static final Character SPACE = ' ';
    private static final String FORMAT_DOUBLE = "%.6f";

    public void writeTable(List<Pair<Double, Double>> table, Path tablePath) throws IOException {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(table.size());
        stringBuilder.append(NEW_LINE);
        for (Pair<Double, Double> row : table) {
            stringBuilder.append(String.format(FORMAT_DOUBLE, row.getValue0()));
            stringBuilder.append(SPACE);
            stringBuilder.append(String.format(FORMAT_DOUBLE, row.getValue1()));
            stringBuilder.append(NEW_LINE);
        }

        try {
            Files.writeString(tablePath, stringBuilder.toString());
        }catch (IOException e) {
            log.info("And error tring to writing table at path {} was occured",tablePath);
            throw e;
        }

    }
}
