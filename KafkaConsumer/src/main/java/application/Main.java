package application;

import application.messageListeners.JpaConfig;
import application.messageListeners.KafkaConfig;
import java.util.concurrent.CountDownLatch;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Main {

  private static final CountDownLatch latch = new CountDownLatch(1);

  public static void main(String[] args) throws InterruptedException {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(JpaConfig.class, KafkaConfig.class);
    context.refresh();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      context.close();
      latch.countDown();
    }));

    System.out.println("App running. Press Ctrl+C to exit.");
    latch.await();
  }
}


