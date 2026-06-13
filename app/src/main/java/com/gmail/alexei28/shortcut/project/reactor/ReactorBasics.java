package com.gmail.alexei28.shortcut.project.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
    Что здесь происходит?
    1. Flux.just(): Создает поток (Flux) из фиксированного набора элементов.
    2. filter(): Оператор, который пропускает только те элементы, которые удовлетворяют условию. Он возвращает новый Flux.
    3. map(): Оператор, который преобразует каждый элемент в потоке, применяя к нему функцию. Он также возвращает новый Flux.
    4. subscribe(): Это самый важный метод. Без него ничего не произойдет! Реактивные потоки "ленивы" — они начинают издавать данные
       только тогда, когда на них подписываются.
        -Первый аргумент subscribe — это обработчик для каждого элемента (onNext).
        -Второй — обработчик ошибок (onError).
        -Третий — обработчик завершения потока (onComplete).
    5. Mono.fromSupplier(): Создает Mono, который будет выполнен асинхронно при подписке.
    Результатом выполнения Supplier будет единственный элемент потока.

    Важное правило: Реактивный поток не начнет выполнять код до тех пор, пока на него не подпишутся с помощью метода subscribe().
 */
public class ReactorBasics {
    private static final Logger logger = LoggerFactory.getLogger(ReactorBasics.class);

    public static void main(String[] args) throws InterruptedException {
        // --- Пример с Flux (поток из множества элементов) ---
        // 1. Создаем Flux из нескольких элементов
        Flux<String> fluxOfStrings = Flux.just("Apple", "Orange", "Banana");

        // 2. Применяем операторы для преобразования данных
        fluxOfStrings
                .filter(fruit -> fruit.length() > 5)      // ← Оставляем только фрукты с длиной имени > 5
                .map(String::toUpperCase)                 // ← Преобразуем в верхний регистр
                .subscribe(                                // ← Подписываемся и обрабатываем конечный результат
                        result -> logger.info("Обработанный фрукт: {}",  result),
                        error -> logger.error("Ошибка: {}", error.getMessage()),
                        () -> System.out.println("Поток данных завершен!")
                );

        logger.info("----");

        // --- Пример с Mono (поток из 0 или 1 элемента) ---

        // 1. Создаем Mono, который будет выполнен асинхронно
        Mono<String> monoOfGreeting = Mono.fromSupplier(() -> {
            // Имитация долгой операции
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return "Hello, Reactive World!";
        });

        // 2. Подписываемся и обрабатываем результат
        monoOfGreeting.subscribe(
                greeting -> System.out.println("Приветствие: " + greeting)
        );

        // Ждем завершения асинхронных операций, так как main поток может завершиться раньше
        Thread.sleep(2000);
    }
}