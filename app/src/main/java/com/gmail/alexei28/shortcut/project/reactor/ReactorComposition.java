package com.gmail.alexei28.shortcut.project.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
Что здесь происходит?
1. then(): Выполняет второй Mono после успешного завершения первого. Результат первого Mono игнорируется, возвращается результат второго.
2. Flux.merge(): Сливает несколько потоков в один. Элементы будут появляться в результирующем потоке по мере их готовности
   из исходных потоков (неупорядоченно).
3. Flux.zip(): Комбинирует элементы из потоков попарно. Он ждет, пока в каждом потоке появится элемент, затем объединяет их в кортеж (Tuple)
   и выдает его. Результирующий поток завершится, когда завершится самый короткий из исходных потоков.
*/
public class ReactorComposition {
    private static final Logger logger = LoggerFactory.getLogger(ReactorComposition.class);

    public static void main(String[] args) throws InterruptedException {
        // --- then (последовательное выполнение) ---
        Mono<String> firstTask = Mono.just("Первая задача");
        Mono<String> secondTask = Mono.just("Вторая задача");

        firstTask
                .then(secondTask) // ← Ждет завершения firstTask, затем запускает secondTask
                .subscribe(result -> logger.info("Результат then: {}", result));

        // --- merge (слияние потоков) ---

        Flux<String> flux1 = Flux.just("A", "B", "C");
        Flux<String> flux2 = Flux.just("D", "E", "F");

        Flux.merge(flux1, flux2) // ← Сливает элементы из обоих потоков по мере их поступления
                .subscribe(item -> System.out.println("Результат merge: " + item));

        // --- zip (комбинирование элементов попарно) ---

        Flux<String> names = Flux.just("Alice", "Bob", "Charlie");
        Flux<Integer> ages = Flux.just(25, 30, 35);

        Flux.zip(names, ages) // ← Комбинирует i-й элемент из первого потока с i-м элементом из второго
                .subscribe(tuple -> logger.info("Имя: {}, Возраст: {}", tuple.getT1(), tuple.getT2()));

        // Ждем завершения, так как здесь могут быть асинхронные операции
        Thread.sleep(1000);
    }
}
