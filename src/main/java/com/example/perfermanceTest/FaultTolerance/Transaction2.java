package com.example.perfermanceTest.FaultTolerance;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data // Lombok: Generates getters, setters, toString, equals, hashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Transaction2 {

    @ToString.Exclude
    private Integer id;

    private String transactionDate;
    private String amount;
    private String createdAt;


}
