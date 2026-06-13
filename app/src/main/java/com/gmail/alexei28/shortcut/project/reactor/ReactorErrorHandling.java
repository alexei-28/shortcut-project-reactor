package com.gmail.alexei28.shortcut.project.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/*
    Что здесь происходит?
    - onErrorReturn(value): Если в потоке происходит ошибка, он немедленно завершается и выдает указанное значение value.
    - onErrorResume(function): Если в потоке происходит ошибка, вместо него выполняется другой реактивный поток, возвращаемый function.
      Это позволяет реализовать сложную логику восстановления.
    - doFinally(callback): Позволяет выполнить блок кода, когда поток завершается по любой причине (успешно, с ошибкой или из-за отмены).
      Идеально подходит для очистки ресурсов.
 */
public class ReactorErrorHandling {
    private static final Logger logger = LoggerFactory.getLogger(ReactorErrorHandling.class);

    public static void main(String[] args) {
        // --- Обработка ошибок ---
        Mono<String> errorMono = Mono.error(new RuntimeException("Что-то пошло не так!"));

        errorMono
                .doOnError(error -> logger.info("Перехват ошибки в doOnError: {}", error.getMessage()))
                .onErrorReturn("Значение по умолчанию") // ← Возвращает альтернативное значение при ошибке
                .subscribe(
                        result -> logger.info("Результат с onErrorReturn: {}", result)
                );

        errorMono
                .onErrorResume(error -> { // ← При ошибке выполняет другой реактивный поток
                    logger.error("Перехват ошибки в onErrorResume: {}", error.getMessage());
                    return Mono.just("Восстановленное значение");
                })
                .subscribe(
                        result -> logger.info("Результат с onErrorResume: {}", result)
                );

        // --- Использование finally (doFinally) ---

        Flux<String> resourceFlux = Flux.just("Resource 1", "Resource 2")
                .doFinally(signalType -> { // ← Выполняется при завершении потока (успех, ошибка, отмена)
                    logger.info("Поток завершен с сигналом: {}", signalType);
                    // Здесь можно освободить ресурсы, например, закрыть соединение с БД
                });

        resourceFlux.subscribe(item -> logger.info("Получен ресурс: {}", item));
    }
}