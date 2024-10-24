package sample;


import com.segment.analytics.Analytics;
import com.segment.analytics.messages.TrackMessage;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PreDestroy;
import com.segment.analytics.Log;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static Analytics analytics;
  private static BlockingFlush blockingFlush;

  public String handleRequest(Map<String, Object> event, Context context) {
        // Example: log and return a response
        context.getLogger().log("Event received: " + event);
         try {
      // Call the main business logic (mainrun)
      mainrun();
    } catch (Exception e) {
      log.error("Error during execution: {}", e.getMessage(), e);
      return "Lambda execution failed with error: " + e.getMessage();
    }
        // Your main business logic goes here
        return "Lambda executed successfully!";
    }


  

  public static void mainrun() throws Exception {
    blockingFlush = BlockingFlush.create();
    int timeoutInSeconds = Integer.parseInt("1000");

// Log STDOUT = new Log() {
//     @Override
//     public void print(Level level, String format, Object... args) {
//         System.out.println(level + ":\t" + String.format(format, args));
//     }

//     @Override
//     public void print(Level level, Throwable error, String format, Object... args) {
//         System.out.println(level + ":\t" + String.format(format, args));
//         System.out.println(error);
//     }
// };

    // Initialize Segment Analytics client
    analytics = Analytics.builder("RfwD1Ao6La0z9KcclV7YvMizfvk9A0Si")
        .plugin(blockingFlush.plugin())
        .plugin(new LoggingPlugin())
        .client(new OkHttpClient.Builder()
            .connectTimeout(Duration.ofSeconds(timeoutInSeconds))
            .readTimeout(Duration.ofSeconds(timeoutInSeconds))
            .writeTimeout(Duration.ofSeconds(timeoutInSeconds))
            .addInterceptor(new GzipRequestInterceptor())
            .build())
        .build();

    // Event tracking logic
    final String userId = System.getProperty("user.name");
    final String anonymousId = UUID.randomUUID().toString();
    final AtomicInteger count = new AtomicInteger();
    
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("count", count.incrementAndGet());
        analytics.enqueue(
            TrackMessage.builder("Java Test")
                .properties(properties)
                .anonymousId(anonymousId)
                .userId(userId)
        );
      }
    }

    analytics.flush();
    blockingFlush.block();
    analytics.shutdown();
  }

  // Method to create and send Segment event for a purchase
  public void createSegmentEvent(Purchase purchase) throws Exception {
    prepareCommonIdentifyEvent(analytics, purchase);
    prepareOrderCompletedTrackEvent(analytics, purchase);

    log.info("Initiating flush for analytics...");
    flushWithRetry();
  }


 private void flushAnalytics() {
    try {
      log.info("Attempting to flush analytics...");
        analytics.flush();
        blockingFlush.block(); // This can throw InterruptedException
        log.info("Flush successful!");
    // } catch (InterruptedException e) {
    //     Thread.currentThread().interrupt(); // Restore interrupted status
    //     throw new RuntimeException("BlockingFlush operation interrupted", e);
    } 
    catch (Exception e) {

      log.error("Error during analytics flush. Exception: {}", e.getMessage(), e);
        throw new RuntimeException("Error during analytics flush", e);
    }
}

  // Retry logic for flush operation
private void flushWithRetry() {
    int retryCount = 0;
    boolean success = false;
    int retryLimit = 5;
    int flushTimeout = 60;

    // Retry loop for handling transient errors
   while (retryCount < retryLimit && !success) {
        CompletableFuture<Void> flushFuture = CompletableFuture.runAsync(() -> {
            flushAnalytics(); // Call flushAnalytics without needing to handle InterruptedException
        });

        try {
            // This may throw TimeoutException and ExecutionException
            flushFuture.get(flushTimeout, TimeUnit.SECONDS);
            success = true; // Mark flush as successful if no exception is thrown
            log.info("Flush completed successfully.");
        } catch (Exception e) {
            retryCount++;
            log.error("Error during flush operation on attempt {}. Retrying...", retryCount, e);
            // You can add additional logging or handling if needed
        }
    }

    // If retries are exhausted, throw an exception
    if (!success) {
        throw new RuntimeException("Failed to flush after " + retryLimit + " retries.");
    }
}




  // Cleanup method to be called before shutdown
  @PreDestroy
  public void cleanup() {
    log.info("Shutting down analytics...");
    analytics.shutdown();
  }

  // Method for preparing common identify event (To be implemented)
  private void prepareCommonIdentifyEvent(Analytics analytics, Purchase purchase) {
    // Implement the logic to prepare an identify event
  }

  // Method for preparing order completed track event (To be implemented)
  private void prepareOrderCompletedTrackEvent(Analytics analytics, Purchase purchase) {
    // Implement the logic to prepare an order completed event
  }
}
