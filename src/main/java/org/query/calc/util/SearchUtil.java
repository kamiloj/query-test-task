package org.query.calc.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.javatuples.Pair;

import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SearchUtil {
    public static int findNextElementFromTargetByBinarySearch(List<Pair<Double, Double>> arr, double target) {
        int start = 0;
        int end = arr.size() - 1;
        int ans = -1;
        while (start <= end) {
            int mid = (start + end) / 2;
            if (arr.get(mid).getValue0() <= target) {
                start = mid + 1;
            } else {
                ans = mid;
                end = mid - 1;
            }
        }
        return ans;
    }
}
