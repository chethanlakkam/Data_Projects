insert into EMP_PROC.emp_tot_hrs_spent
select D.Essn,Dependent_name,Pnumber, SUM(Hours) from EMP_RAW.Dependent D
Join EMP_RAW.Works_ON W
On D.Essn = W.Essn
JOIN EMP_RAW.Project P
ON W.Pno= P.Pnumber
group by D.Essn,P.Pnumber,Dependent_name
HAVING COUNT(Dependent_name) > 0;