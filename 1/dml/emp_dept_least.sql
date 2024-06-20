insert into EMP_PROC.emp_dept_least
select D.Dname, D.Dnumber ,Count(*)from EMP_RAW.Department D
left Join EMP_RAW.Employee E
ON D.Dnumber = E.Dno
group by Dname,Dnumber
order by 3 LIMIT 1;