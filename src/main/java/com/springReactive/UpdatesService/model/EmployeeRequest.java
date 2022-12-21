package com.springReactive.UpdatesService.model;

import lombok.*;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class EmployeeRequest {
    private int empId;

    private String empName;

    private String empCity;

    private String empPhone;

    private double javaExp;

    private double springExp;

}
