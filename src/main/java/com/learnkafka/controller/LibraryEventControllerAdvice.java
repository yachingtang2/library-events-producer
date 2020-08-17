package com.learnkafka.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
public class LibraryEventControllerAdvice {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<?> handleRequestBody(MethodArgumentNotValidException exception) {
    List<FieldError> errors = exception.getBindingResult().getFieldErrors();
    String errorMessage = errors.stream()
        .map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage())
        .sorted()
        .collect(Collectors.joining(", "));
    log.info("Error message: {}", errorMessage);
    return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
  }
}
