package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK SPORADIC: tratează alarmele de temperatură.
 *
 * Evenimentele sosesc în coada alarmEvents (puse de taskul periodic).
 * Intervalul minim între sosiri este MIN_INTERARRIVAL_MS: între două eliberări
 * ale taskului (două alarme publicate) trebuie să treacă cel puțin atât.
 * Intervalul este impus de cod: evenimentele care sosesc mai des sunt
 * respinse, ca să nu fie inundat brokerul.
 */
public class SporadicAlarmTask implements Runnable {

    static final long MIN_INTERARRIVAL_MS = 5000;

    private final String deviceId;
    private final MqttConnection mqtt;
    private final BlockingQueue<Double> alarmEvents;
    private final PeriodicTelemetryTask periodicTask;

    public SporadicAlarmTask(String deviceId, MqttConnection mqtt, BlockingQueue<Double> alarmEvents, PeriodicTelemetryTask periodicTask) {
        this.deviceId = deviceId;
        this.mqtt = mqtt;
        this.alarmEvents = alarmEvents;
        this.periodicTask = periodicTask;
    }

    @Override
    public void run() {
        // TODO 2: implementați taskul sporadic:
        //   - așteptați un eveniment: alarmEvents.take()
        //   - dacă de la ultima eliberare au trecut mai puțin de
        //     MIN_INTERARRIVAL_MS, respingeți evenimentul (afișați un mesaj)
        //   - altfel publicați pe Config.alarmTopic(deviceId) mesajul
        //     "deviceId;timestamp;temperatura;prag"
        //     (pragul curent poate fi transmis împreună cu temperatura sau
        //      citit din PeriodicTelemetryTask; alegeți și justificați în raport)
        //
        // Vezi exemplul exemple/01-taskuri/.../SporadicTaskExample.java
        long lastReleaseTime = 0; // Pastreaza momentul ultimei alarme acceptate

        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 1. Preluam evenimentul din coada, take() blocheaza thread-ul pana apare o valoare
                Double temperatura = alarmEvents.take();
                long currentTime = System.currentTimeMillis();

                // 2. Interval minim intre sosiri
                if (currentTime - lastReleaseTime >= MIN_INTERARRIVAL_MS) {
                    lastReleaseTime = currentTime; //Actualizare timp pt ultima alarma acceptata

                    // 3. Construim si publicam mesajul MQTT in formatul cerut
                    String topic = Config.alarmTopic(deviceId);
                    String tempFormata = String.format(java.util.Locale.US, "%.2f", temperatura);
                    String pragFormatat = String.format(java.util.Locale.US, "%.2f", periodicTask.getThreshold());
                    String payload = deviceId + ";" + currentTime + ";" + tempFormata + ";" + pragFormatat;

                    mqtt.publish(topic, payload);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }


    }
}
