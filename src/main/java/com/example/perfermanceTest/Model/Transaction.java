package com.example.perfermanceTest.Model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data // Lombok: Generates getters, setters, toString, equals, hashCode
@NoArgsConstructor
@AllArgsConstructor
@Builder

public class Transaction {

    private Integer id;
    private LocalDate transactionDate; // Matches DATE type
    private BigDecimal amount;
    private LocalDate createdAt;


}
