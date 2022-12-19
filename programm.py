from datetime import   datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from random import randrange
import os

def hello():
    print("Airlow")

# workflow поток
def rand():
    a = randrange(100)
    b = randrange(100)
    print(a, " ", b)      

    # Сумма должна затираться
    
    if(os.stat("note.txt").st_size != 0):
        fd=open("note.txt","r")
        d=fd.read()
        fd.close()
        m=d.split("\n")
        s="\n".join(m[:-1])
        fd=open("note.txt","w+")
        for i in range(len(s)):
            fd.write(s[i])
        fd.close()
    
    f = open('note.txt', 'a')
    s = str(a) +  " " +  str(b) +'\n'
    f.write(s)
    f.close()
    
def  filework():
    s1=0
    s2=0
    with open('note.txt') as f:
        for line in f.readlines():
            line = line.strip("\n").split()
            s1 += int(line[0])
            s2 += int(line[1])
    
    with open('note.txt','a') as f:
        f.write(str(s2-s1))     
        



 

with DAG(dag_id="first_dag", start_date=datetime(2022,1,1), schedule="0-5 * * * *") as dag:
    bash_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task = PythonOperator(task_id="world", python_callable = hello)
    python_task2 = PythonOperator(task_id="random", python_callable = rand)
    python_task3 = PythonOperator(task_id="filewr", python_callable = filework)
    
       
        
    bash_task >> python_task
    bash_task >> python_task2
    bash_task >> python_task3
