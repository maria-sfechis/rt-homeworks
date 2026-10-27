package ssatr.service;

import java.util.concurrent.BlockingQueue;

/**
 * Preia mesajele de un anumit tip ("telemetry", "alarm" sau "status") din coada
 * inbox, le salvează și actualizează dashboard-ul.
 *
 * Formatul mesajelor (câmpuri separate prin ";"):
 *   telemetry:  deviceId;timestamp;temperatura
 *   alarm:      deviceId;timestamp;temperatura;prag
 *   status:     deviceId;timestamp;text
 */
public class MessageProcessor implements Runnable {

    private final String kind;
    private final BlockingQueue<String> inbox;
    private final CsvStorage storage;
    private final Dashboard dashboard;

    public MessageProcessor(String kind, BlockingQueue<String> inbox, CsvStorage storage, Dashboard dashboard) {
        this.kind = kind;
        this.inbox = inbox;
        this.storage = storage;
        this.dashboard = dashboard;
    }

    @Override
    public void run() {
        // TODO 4: într-o buclă:
        //   - preluați un mesaj: inbox.take()
        //   - salvați mesajul în fișierul CSV: storage.append(kind, mesaj)
        //     (CsvStorage este gata implementată)
        //   - actualizați dashboard-ul, în funcție de kind
        //   - alarmele și confirmările se afișează imediat în consolă
        //   - un mesaj greșit (format invalid) nu trebuie să oprească thread-ul
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 1. Preia mesajul
                String mesaj = inbox.take();

                // 2. Try-catch intern care sa nu opreasca un thread la mesaj invalid
                try {
                    // Salvam in CSV
                    storage.append(kind, mesaj);
                    // Extragem elementele de baza
                    String[] parts = mesaj.split(";");
                    String deviceId = parts[0];
                    // Actualizare dashboard si afisare consola in functie de tip
                    switch (kind) {
                        case "telemetry":
                            long timestamp = Long.parseLong(parts[1]);
                            double temp = Double.parseDouble(parts[2]);
                            dashboard.updateTelemetry(deviceId, temp, timestamp);
                            break;
                        case "alarm":
                            dashboard.incrementAlarms(deviceId);
                            System.out.println("\n[ALARMA] " + deviceId + " a raportat depasirea pragului: " + mesaj);
                            break;
                        case "status":
                            System.out.println("\n[STATUS] Confirmare de la " + deviceId + ": " + mesaj);
                            break;
                    }
                } catch (Exception e) {
                    System.err.println("Eroare de format la procesarea mesajului (" + kind + "): " + mesaj);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
