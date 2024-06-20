insert into EMP_PROC.emp_project_dept
SELECT W.Essn, P.Pname, D.Dname AS proj_dept_name, ED.Dname AS emp_dept_name
FROM EMP_RAW.WORKS_ON W
LEFT JOIN EMP_RAW.PROJECT P ON W.Pno = P.Pnumber
JOIN EMP_RAW.DEPARTMENT D ON P.Dnum = D.Dnumber
JOIN EMP_RAW.EMPLOYEE E ON W.Essn = E.ssn
JOIN EMP_RAW.DEPARTMENT ED ON E.dno = ED.Dnumber
WHERE P.Dnum != E.dno
GROUP BY W.Essn,P.Pname,D.Dname,ED.Dname;