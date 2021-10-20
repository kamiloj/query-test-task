package org.query.calc;

import lombok.extern.java.Log;
import one.util.streamex.StreamEx;
import org.javatuples.Pair;
import org.query.calc.persistence.TableReader;
import org.query.calc.persistence.TableWritter;
import org.query.calc.util.SearchUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Log
public class QueryCalcImpl implements QueryCalc {

    private static final int QUERY_LIMIT = 10;
    private static final Function<Map.Entry<Pair<Double, Double>, Pair<Double, Double>>,
            Pair<Double, Double>> REDUCE_T2_CROSS_T3 =
            entry -> new Pair<>(entry.getValue().getValue0() + entry.getKey().getValue0(),
                    entry.getValue().getValue1() * entry.getKey().getValue1());

    private final TableReader tableReader = new TableReader();
    private final TableWritter tableWritter = new TableWritter();

    @Override
    public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
        List<Pair<Double, Double>> table1 = tableReader.readTable(t1);
        List<Pair<Double, Double>> table2 = tableReader.readTable(t2);
        List<Pair<Double, Double>> table3 = tableReader.readTable(t3);

        List<Pair<Double, Double>> sortedTable2CrossJoinTable3 = getSortedTableCrossJoinTable(table2, table3);
        setSumProductTable(sortedTable2CrossJoinTable3);
        Map<Double, Double> mapResult = getFullQueryResult(table1, sortedTable2CrossJoinTable3);

        final List<Pair<Double, Double>> resultQuery = orderAndLimitResult(mapResult);

        tableWritter.writeTable(resultQuery, output);
    }

    private List<Pair<Double, Double>> getSortedTableCrossJoinTable(List<Pair<Double, Double>> table1, List<Pair<Double, Double>> table2) {
        return StreamEx.of(table1)
                .cross(table2)
                .map(REDUCE_T2_CROSS_T3)
                .sorted(Comparator.comparingDouble(Pair::getValue0))
                .collect(Collectors.toList());
    }

    private void setSumProductTable(List<Pair<Double, Double>> table) {
        double sumAccumulated = 0;
        table.add(new Pair<>(Double.MAX_VALUE, 0.0));

        for (int i = table.size() - 1; i >= 0; i--) {
            Pair<Double, Double> a = table.get(i);
            sumAccumulated += a.getValue1();
            table.set(i, new Pair<>(a.getValue0(), sumAccumulated));
        }
    }

    private Map<Double, Double> getFullQueryResult(List<Pair<Double, Double>> table1, List<Pair<Double, Double>> table2) {
        final Map<Double, Double> mapResult = new LinkedHashMap<>();

        for (Pair<Double, Double> iteratorT1 : table1) {
            int indexNextElement = SearchUtil.findNextElementFromTargetByBinarySearch(table2, iteratorT1.getValue0());
            double partialSum = mapResult.getOrDefault(iteratorT1.getValue0(), 0.0);
            partialSum += table2.get(indexNextElement).getValue1() * iteratorT1.getValue1();
            mapResult.put(iteratorT1.getValue0(), partialSum);
        }

        return mapResult;
    }

    private List<Pair<Double, Double>> orderAndLimitResult(Map<Double, Double> mapResult) {
        return mapResult.entrySet()
                .stream()
                .sorted(Map.Entry.<Double, Double>comparingByValue().reversed())
                .limit(QUERY_LIMIT)
                .map(entry -> new Pair<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
}
