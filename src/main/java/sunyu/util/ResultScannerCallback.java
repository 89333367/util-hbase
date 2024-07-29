package sunyu.util;

import java.util.Map;

public interface ResultScannerCallback {
    void execute(Map<String, String> row);
}
