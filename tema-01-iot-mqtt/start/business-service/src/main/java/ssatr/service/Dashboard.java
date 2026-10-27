package ssatr.service;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Ultima stare cunoscută a fiecărui device: ultima temperatură, momentul
 * ultimului mesaj, numărul de alarme etc.
 *
 * Este actualizată de thread-urile MessageProcessor și citită de CommandConsole,
 * deci accesul trebuie sincronizat.
 */
public class Dashboard {
    // TODO 5: alegeți structura de date pentru starea device-urilor
    //   (de exemplu un Map<String, ...> cu cheia deviceId)
    //   și adăugați metodele de actualizare apelate din MessageProcessor.

    private static class DeviceState {
        double lastTemp;
        long lastUpdate;
        int alarmCount = 0;
    }

    private final Map<String, DeviceState> states = new ConcurrentHashMap<>();
    public void updateTelemetry(String deviceId, double temp, long timestamp) {
        states.compute(deviceId, (k, state) -> {
            if (state == null) {
                state = new DeviceState();
            }
            state.lastTemp = temp;
            state.lastUpdate = timestamp;
            return state;
        });
    }

    public void incrementAlarms(String deviceId) {
        states.computeIfPresent(deviceId, (k, state) -> {
            state.alarmCount++;
            return state;
        });
    }
    /** Afișează un tabel cu starea tuturor device-urilor. */
    public void print() {
        // TODO 5: afișați câte un rând pentru fiecare device, de exemplu:
        //   DEVICE     TEMP    ULTIMUL MESAJ   ALARME
        //   device-1   23.41   acum 1 s        2
        System.out.println("\nDEVICE\t\tTEMP\tULTIMUL MESAJ\tALARME");
        long now = System.currentTimeMillis();

        for (Map.Entry<String, DeviceState> entry : states.entrySet()) {
            String deviceId = entry.getKey();
            DeviceState state = entry.getValue();
            // Vechimea ultimului mesaj in secunde
            long ageSeconds = (now - state.lastUpdate) / 1000;
            System.out.printf("%s\t%.2f\tacum %d s\t%d%n", deviceId, state.lastTemp, ageSeconds, state.alarmCount);
        }
    }
}
