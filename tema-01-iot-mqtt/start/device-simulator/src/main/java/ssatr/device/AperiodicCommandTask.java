package ssatr.device;

import java.util.concurrent.BlockingQueue;

/**
 * TASK APERIODIC: execută comenzile primite de la business service.
 *
 * Comenzile sosesc oricând, pe MQTT, fără un interval minim între ele, și sunt
 * puse în coada commands. Fiecare comandă eliberează un job al taskului.
 * Comenzi care trebuie suportate:
 *
 *   SET_INTERVAL <ms>       schimbă perioada T a taskului periodic
 *   SET_THRESHOLD <valoare> schimbă pragul de alarmă
 *   RESET                   readuce senzorul la valoarea inițială
 *   STATUS                  raportează perioada și pragul curente
 *
 * După executarea fiecărei comenzi, device-ul publică pe Config.statusTopic(deviceId)
 * o confirmare: "deviceId;timestamp;text", de exemplu
 * "device-1;1727600000000;OK SET_INTERVAL 1000".
 */
public class AperiodicCommandTask implements Runnable {

    private final String deviceId;
    private final TemperatureSensor sensor;
    private final MqttConnection mqtt;
    private final BlockingQueue<String> commands;
    private final PeriodicTelemetryTask periodic;

    public AperiodicCommandTask(String deviceId, TemperatureSensor sensor, MqttConnection mqtt,
                                BlockingQueue<String> commands, PeriodicTelemetryTask periodic) {
        this.deviceId = deviceId;
        this.sensor = sensor;
        this.mqtt = mqtt;
        this.commands = commands;
        this.periodic = periodic;
    }

    @Override
    public void run() {
        //periodic.setPeriodMS(valoare);
        // TODO 3: implementați taskul aperiodic:
        //   - preluați comenzile din coadă: commands.take()
        //   - separați numele comenzii de parametru: command.split(" ")
        //   - executați comanda (periodic.setPeriodMs(...), sensor.reset() etc.)
        //   - publicați confirmarea pe Config.statusTopic(deviceId)
        //   - o comandă necunoscută sau un parametru invalid nu trebuie să
        //     oprească taskul: publicați "ERROR <motiv>"
        //
        // Vezi exemplul exemple/01-taskuri/.../AperiodicTaskExample.java
        try {
            while (!Thread.currentThread().isInterrupted()) {
                // 1. Preluam comanda din coada
                String commandLine = commands.take();
                String responsePayload;

                // Folosim try-catch interior pentru a preveni oprirea task-ului la comanda invalida
                try {
                    String[] parts = commandLine.split(" ");
                    String cmd =  parts[0];
                    switch (cmd) {
                        case "SET_INTERVAL":
                            long newInterval = Long.parseLong(parts[1]);
                            periodic.setPeriodMs(newInterval);
                            responsePayload = "OK " + commandLine;
                            break;
                        case "SET_THRESHOLD":
                            double newThreshold = Double.parseDouble(parts[1]);
                            periodic.setThreshold(newThreshold);
                            responsePayload = "OK " + commandLine;
                            break;
                        case "RESET":
                            sensor.reset();
                            responsePayload = "OK " + commandLine;
                            break;
                        case "STATUS":
                            responsePayload = "OK STATUS T = " + periodic.getPeriodMs() + " threshold = " + periodic.getThreshold();
                            break;
                        default:
                            responsePayload = "ERROR unknown command: " + cmd;
                    }
                } catch (Exception e) {
                    responsePayload = "ERROR invalid command format: " + commandLine;
                }
                // 2. Construim si publicam mesajul de status cerut in formatul cerut
                String topic = Config.statusTopic(deviceId);
                long timestamp = System.currentTimeMillis();
                String finalMessage = deviceId + ";" + timestamp + ";" + responsePayload;

                mqtt.publish(topic, finalMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
