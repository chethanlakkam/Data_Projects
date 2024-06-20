insert into EMP_PROC.emp_full_details
SELECT 
    E.ssn, 
    E.fname, 
    E.minit, 
    E.lname, 
    E.bdate, 
    E.address, 
    E.sex, 
    E.salary, 
    E.super_ssn, 
    E.dno,
    D.dname, 
    DL.dlocation, 
    P.pname, 
    P.pnumber, 
    P.plocation,
    COALESCE(SUM(W.hours), 0) AS total_hours, 
    DE.dependent_name, 
    DE.sex AS dependent_sex, 
    DE.relationship AS dependent_relation
FROM 
    EMP_RAW.employee E
LEFT JOIN 
    EMP_RAW.department D ON E.dno = D.dnumber
LEFT JOIN 
    EMP_RAW.dept_locations DL ON D.dnumber = DL.dnumber
LEFT JOIN 
    EMP_RAW.project P ON D.dnumber = P.dnum
LEFT JOIN 
    EMP_RAW.works_on W ON E.ssn = W.essn AND P.pnumber = W.pno
LEFT JOIN 
    EMP_RAW.dependent DE ON E.ssn = DE.essn
GROUP BY 
    E.ssn, E.fname, E.minit, E.lname, E.bdate, E.address, E.sex, E.salary, E.super_ssn, 
    E.dno, D.dname, DL.dlocation, P.pname, P.pnumber, P.plocation, DE.dependent_name, DE.sex, DE.relationship;
