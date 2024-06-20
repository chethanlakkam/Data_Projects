create or replace table P01_EMP.EMP_PROC.emp_full_details(essn char(9),fname varchar(30),
	minit char(1),lname varchar(30),
    bdate date,address varchar(30),sex char(1),salary decimal(10,2),
    super_ssn char(9),dno smallint,dname varchar(30),dlocation varchar(20),
    pname varchar(30),pnumber smallint,plocation varchar(30),total_hours decimal(4,2),
    dependent_name varchar(30), dependent_sex char(1),
    dependent_relation varchar(20));