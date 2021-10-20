package org.query.calc.persistence;

import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TableReader {
    private static final String SPACE = " ";

    public List<Pair<Double, Double>> readTable(Path tablePath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(tablePath)) {
            String line = reader.readLine();

            int rows = Integer.parseInt(line);
            List<Pair<Double, Double>> list = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                line = reader.readLine();
                String[] split = line.split(SPACE);
                list.add(new Pair<>(Double.parseDouble(split[0]), Double.parseDouble(split[1])));
            }
            return list;
        } catch (IOException e) {
            log.info("And error tring to load table at path {} was occured",tablePath);
            throw e;
        }
    }
}
