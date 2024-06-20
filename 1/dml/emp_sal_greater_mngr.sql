insert into EMP_PROC.emp_sal_greater_mngr
select E.ssn,E.salary,E.Super_ssn,F.salary from EMP_RAW.employee E
LEFT JOIN EMP_RAW.employee F
ON E.Super_ssn = F.ssn
WHERE E.salary> F.Salary 
group by E.ssn,E.salary,E.Super_ssn,F.salary;