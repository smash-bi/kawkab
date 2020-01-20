package kawkab.fs.utils;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import kawkab.fs.commons.Stats;

public class GCMonitor implements NotificationListener {
    private static ConcurrentLinkedQueue<GCEvent> gcEvents = new ConcurrentLinkedQueue<GCEvent>();
    private static Stats durationStats = new Stats();

    public class GCEvent {
        public long gcTime;
        public long duration;
        public long startTime;
        public long endTime;

        //All are in milliseconds
        GCEvent(long gcTime, long startTime, long endTime, long duration) {
            this.gcTime = gcTime;
            this.startTime = startTime;
            this.endTime = endTime;
            this.duration = duration;
        }
    }
    
    private static GCMonitor instance = null;
    public synchronized static GCMonitor getInstance(){
        if (instance == null)
            instance = new GCMonitor();
        return instance;
    }
    
    public synchronized static void initialize(){
        getInstance();
    }

    private GCMonitor() {
        // probably two - the old generation and young generation
        List<GarbageCollectorMXBean> gcbeans = java.lang.management.ManagementFactory
                .getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcbean : gcbeans) {
            NotificationEmitter emitter = (NotificationEmitter) gcbean;
            emitter.addNotificationListener(this, null, null);
        }
    }

    @SuppressWarnings("restriction")
	@Override
    public void handleNotification(Notification notification, Object handback) {
        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            // get the information associated with this notification
            GarbageCollectionNotificationInfo gcInfo = GarbageCollectionNotificationInfo.from(
                    (CompositeData) notification.getUserData());
            GcInfo info = gcInfo.getGcInfo();
            // get all the info and pretty print it
            long duration = info.getDuration();
            System.out.printf("    GC: %dms\n", duration);
            Map<String, MemoryUsage> befMem = info.getMemoryUsageBeforeGc();

            if (befMem.containsKey("PS Eden Space")) {
                Map<String, MemoryUsage> aftMem = info.getMemoryUsageAfterGc();

                System.out.printf("Eden: %dM->%dM, ",
                        befMem.get("PS Eden Space").getUsed() / 1048576,
                        aftMem.get("PS Eden Space").getUsed() / 1048576);
                System.out.printf("Survivor: %dM->%dM, ",
                        befMem.get("PS Survivor Space").getUsed() / 1048576,
                        aftMem.get("PS Survivor Space").getUsed() / 1048576);
                System.out.printf("Old: %dM->%dM",
                        befMem.get("PS Old Gen").getUsed() / 1048576,
                        aftMem.get("PS Old Gen").getUsed() / 1048576);

                System.out.println();
            }
            
            gcEvents.add(new GCEvent(System.currentTimeMillis(), info.getStartTime(), info.getEndTime(), duration));
            durationStats.putValue(duration);
        }
    }
    
    public static Collection<GCEvent> getEventsList(){
        return Collections.unmodifiableCollection(gcEvents);
    }
    
    public static void printEvents() {
		for (GCEvent event : gcEvents) {
            SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
            Date date = new Date(System.currentTimeMillis());
			System.out.printf("%s, %d, %d, %d\n", formatter.format(date), event.duration, event.startTime, event.endTime);
		}
		System.out.println();
    }
    
    public static void printDurations() {
    	for (GCEvent event : gcEvents) {
			System.out.printf("%d\n", event.duration);
		}
		System.out.println();
    }
    
    public static void printStats() {
        System.out.println(durationStats);
    }

    public static void resetStats() {
        durationStats.clear();
    }

    public static String getStats() {
        return durationStats.toString();
    }
}
