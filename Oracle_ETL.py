import cx_Oracle


try:
   con=  cx_Oracle.connect(user="new_db", password='123', dsn="localhost:1521/orcl",encoding="UTF-8")

except cx_Oracle.DatabaseError as er:
	print('There is an error in the Oracle database:', er)

else:
	try:
		cur = con.cursor()
		
		# sample 1 :
		# fetchone() is used to fetch all the reselts from select query .
		v0=0
		query1 ='Select emp.employee_id,emp.employee_name,emp.phone_number emp_ph_no,emp.hire_date emp_hire_dt,emp.job_id emp_job_id,emp.salary emp_salary,dept.DEPARTMENT_name emp_department_name,mng.employee_id Manager_id ,mng.employee_name Manager_Name,mng.phone_number mng_ph_no ,mng.hire_date mng_hire_dt ,mng.job_id mng_job_id ,mng.salary mng_salary ,mng_dept.DEPARTMENT_name  mng_department_name from employees emp, employees mng , departments dept , departments mng_dept where  emp.manager_id =mng.employee_id (+) and emp.department_id = dept.department_id and mng.department_id= mng_dept.department_id (+) order by EMP.EMPLOYEE_ID,emp.MANAGER_ID nulls first'
		cur.execute(query1)
		result  = cur.fetchall()
		count = len(result) 
		print('Sample 1:\n')
		for x in result :
		 while v0 <= count:
		  v0+=1
		  if v0 > count : 
		    break
		  else :
		   print('Row'+str(v0) +': = %r' % (x,)) 
		   #print('Output'+ str(output))

		#sample 2 :
		# fetchone() is used fetch one record from top of the result set
		query2 ='Select distinct c.REPNO,trim(c.SALESREP),sr.ssn from CUSTOMERSCD  c LEFT  OUTER JOIN  D1_SALESREPS sr ON trim(c.SALESREP) =trim(sr.SALESREP) order by c.repno'
		cur.execute(query2)
		rows = cur.fetchone()
		print('Sample 2:\n')
		print(rows)
		# fetchall() is used to fetch all records from result set
		
		#sample 3 :
		v1=0
		fno=3
		query3='Select m.EmpName,m.month,m.year, m.Maximum_Sales from (select Employee.EmpName,month.month,month.year,max( sales.commision ) Maximum_Sales, row_number() over (partition by Employee.EmpName order by max( sales.commision )  desc) as Rank From Employee,Month,Sales where Employee.EmpId = Sales.empid and sales.monthid =month.monthid group by Employee.EmpName,month.month,month.year) m where m.rank <=2 order by year desc ,month asc'
		# fetchmany(int) is used to fetch limited number of records from result set based on integer argument passed in it
		cur.execute(query3)
		rows = cur.fetchmany(fno)
		print('Sample 3:\n')
		for output in rows :
		 while v1 < fno:
		  v1+=1
		  if v1>fno : 
		    break
		  else :
		   #print('Output row'+str(v1) +'= %r' % (output,))
		   print('Row'+str(v1)+':'+str(output))
	except cx_Oracle.DatabaseError as er:
		print('There is an error in the Oracle database:', er)

	except Exception as er:
		print('Error:'+str(er))

	finally:
		if cur:
			cur.close()
finally:
	if con :
		 con.close()
