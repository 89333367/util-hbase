package sunyu.util;

public abstract class ResultScannerRunnerCallback implements ResultScannerCallback {
    private Boolean running = true;

    public Boolean getRunning() {
        return running;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }

    public ResultScannerRunnerCallback() {
        this.running = running;
    }

}
