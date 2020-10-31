#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing a hybrid 
#	solution that finds the trade-off 
#	between latency and data quality check 
#	in a geo-distributed, heterogeneous and
#	edge-computing environment. 
#-------------------------------------------#
from pulp import *
import random
import time
from array import *
from random import choices
from Graph import Graph
import numpy as np
import copy
from scipy.optimize import minimize
from LPOptimizer import LPOptimizer
from DAGCreator import DAGCreator
from SpringRelaxation import SpringRelaxation
from EqualAssignment import EqualAssignment
import time
import sys

class Equality():
	def divideNumber(parts):#Function to divide a random into n unequal parts
		sumPercentages=0
		while(sumPercentages==0):
			randomPercentages=[]
			dividedNumberParts=[]
			for i in range(parts):
				if(random.randint(0,5)>3):
					randomPercentages.append(random.randint(0,10))
					sumPercentages+=randomPercentages[i]
				else:
					randomPercentages.append(0)
		for i in range(parts):
			dividedNumberParts.append(round(randomPercentages[i]/sumPercentages,2))
		return dividedNumberParts
		
	global numberOfDevices	

	#ARG1:number of operators-tested values: 5, 10, 15
	#ARG2:beta for F formula-tested values: 0.5, 1, 1.5, 2, 3 
	#ARG3:alpha penalty (the bigger the value the smaller the penalty)-tested values: 5, 10
	#ARG4:threshold for qualOpt-tested values: 075, 0.8, 0.85, 0.9, 0.95
	#ARG5:number of iterations
	f = open("F.txt", "a")
	f.write("n="+sys.argv[1]+", b="+sys.argv[2]+", a="+sys.argv[3]+", threshold="+sys.argv[4]+", iterations="+sys.argv[5]+"\n")
	f.close()
	f = open("DQ.txt", "a")
	f.write("n="+sys.argv[1]+", b="+sys.argv[2]+", a="+sys.argv[3]+", threshold="+sys.argv[4]+", iterations="+sys.argv[5]+"\n")
	f.close()
	f = open("time.txt", "a")
	f.write("n="+sys.argv[1]+", b="+sys.argv[2]+", a="+sys.argv[3]+", threshold="+sys.argv[4]+", iterations="+sys.argv[5]+"\n")
	f.close()
	#DEFINE GRAPH
	for iter1 in range(1,8):
		if (iter1 == 5):
			f = open("F.txt", "a")
			f.write("Replicated" + "\n")
			f.close()
			f = open("DQ.txt", "a")
			f.write("Replicated" + "\n")
			f.close()
			f = open("time.txt", "a")
			f.write("Replicated" + "\n")
			f.close()
		elif (iter1 == 6):
			f = open("F.txt", "a")
			f.write("Diamond" + "\n")
			f.close()
			f = open("DQ.txt", "a")
			f.write("Diamond" + "\n")
			f.close()
			f = open("time.txt", "a")
			f.write("Diamond" + "\n")
			f.close()
		elif (iter1 == 7):
			f = open("F.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
			f = open("DQ.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
			f = open("time.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
		else:
			 f = open("F.txt", "a")
			 f.write("Medium "+str(iter1)+ "\n")
			 f.close()
			 f = open("DQ.txt", "a")
			 f.write("Medium "+str(iter1) +"\n")
			 f.close()
			 f = open("time.txt", "a")
			 f.write("Medium "+str(iter1)+ "\n")
			 f.close()
		numberOfIterations=int(sys.argv[5])
		for iter2 in range(numberOfIterations):
			numberOfOperators=int(sys.argv[1])
			qThreshold=float(sys.argv[4])

			if(iter1==5):
				if(numberOfOperators==5):
					numberOfOperators=1
				elif(numberOfOperators==10):
					numberOfOperators=2
				else:
					numberOfOperators=3
				dag=DAGCreator("R",numberOfOperators)
			elif(iter1==6):
				numberOfOperators=numberOfOperators-2
				dag=DAGCreator("D",numberOfOperators)
			elif(iter1==7):
				numberOfOperators=numberOfOperators-2
				dag=DAGCreator("S",numberOfOperators)
			else:
				dag = DAGCreator("M", iter1)
			(paths,pairs,source,sink,children,parents,numberOfOperators,noOfSources)=dag.run()
			
			numberOfDevices=numberOfOperators*5
			
			#Set constraints
			#Communication cost between devices (random)
			meancomCost=0
			sumComCost=0
			comCost = [[0 for i in range(numberOfDevices)] for j in range(numberOfDevices)] 

			for i in range(numberOfDevices):
				for j in range(numberOfDevices):
					cost=round(random.uniform(0.1,10),2)#Bounds
					if(i==j):
						comCost[i][j]=0
					if(i<j and i!=j):
						sumComCost=sumComCost+cost
						comCost[i][j]=cost
						comCost[j][i]=cost
			meancomCost=round(sumComCost/((numberOfDevices*(numberOfDevices-1))/2),2)#Used for penalty
			
			#Requirements of operators and DQ
			RCpu=[]
			RMem=[]
			for i in range(numberOfOperators):
				RCpu.append(round(random.uniform(10, 100),2))
				RMem.append(random.randint(100, 1000))
			RCPUDQ=round(random.uniform(10, 100),2)
			RRAMDQ=random.randint(100, 1000)
	
			#Characteristics of devices
			CCpu=[]
			CMem=[]
			UsedCpu=[]
			UsedMem=[]
			for i in range(numberOfDevices):
				CCpu.append(round(random.uniform(10, 100),2))
				CMem.append(random.randint(100, 1000))
				UsedCpu.append(0)
				UsedMem.append(0)
				
			beta=float(sys.argv[2])#For F formula
			alpha=meancomCost/int(sys.argv[3])#Penalty
			
			#Availability of devices per operator
			available=[]
			population = [1, 0]
			weights = [0.85, 0.15]#85% available, 15% not available
			for i in range(numberOfOperators):
				available.append([])
				if (i not in source):
					for j in range(numberOfDevices):
						available[i].append(choices(population, weights)[0])
			
			fractions=[]
			communicationCost=[]
			for n in range(numberOfOperators):
				fractions.append([])
				communicationCost.append(-1)
			
			#Random distribution of sources to devices 
			for op in source:
				flag=0
				while(flag==0):
					tempF=divideNumber(numberOfDevices)
					flag=1
					for dev in range(numberOfDevices):
						if(tempF[dev]*RCpu[op]>CCpu[dev] or tempF[dev]*RMem[op]>CMem[dev]):
							flag=0
				fractions[op]=copy.deepcopy(tempF)
				for dev in range(numberOfDevices):
					if (fractions[op][dev]==0):
						available[op].append(0)
					else:
						available[op].append(1)
					UsedCpu[dev]=UsedCpu[dev]+fractions[op][dev]*RCpu[op]
					UsedMem[dev]=UsedMem[dev]+fractions[op][dev]*RMem[op]
					
			#Spring relaxation
			sr=SpringRelaxation()
			(DQfractionSpring,totalTransferTimeSrping,FSpring)=sr.run(numberOfDevices,comCost,numberOfOperators,paths,pairs,source,sink,children,parents,RCpu,RMem,CCpu,CMem,available,fractions,RCPUDQ,RRAMDQ,noOfSources,beta,alpha)	

			#Εqual distribution
			eq=EqualAssignment()
			(DQfractionEQ,totalTransferTimeEQ,FEQ,UsedCpuEQ,UsedMemEQ,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,slowestPathEQ)=eq.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents,paths,source,noOfSources,fractions,beta,alpha)
			
			#LP initial solution
			transferTimes={}
			slowestDevices={}
			for op in range(numberOfOperators):
				if(op not in source):#For each non source
					problem = LpProblem("problemName", LpMinimize)
					transferTime = LpVariable("transferTime")
					fraction = LpVariable.dicts("x", list(range(numberOfDevices)), 0, 100, cat="Integer")
					for i in range(numberOfDevices):#Constraints
						problem+=(fraction[i]*RCpu[op]/100)<=(CCpu[i]-UsedCpu[i])
						problem+=(fraction[i]*RMem[op]/100)<=(CMem[i]-UsedMem[i])
					c1=0
					for i in range(numberOfDevices):
						c1+=fraction[i]#Sum of fractions equal to 100
						if(available[op][i]==0):
							problem+=fraction[i]==0
					problem+=c1==100
					c2=0
					for op1 in parents[op]:
						for dev1 in range(numberOfDevices):
							for dev2 in range(numberOfDevices):
								c2=c2+((fractions[op1][dev1])*pairs[op1,op]*comCost[dev1][dev2]*fraction[dev2])/100	
					transferTime=c2
					problem+=transferTime
					solved=problem.solve()#Solve LP
					if(solved==-1):
						break;
					#Enforce solution
					for i in range(numberOfDevices):
						fractions[op].append(round(fraction[i].varValue)/100)
						UsedCpu[i]=UsedCpu[i]+(fraction[i].varValue*RCpu[op]/100)
						UsedMem[i]=UsedMem[i]+(fraction[i].varValue*RMem[op]/100)
			
					
			if(solved!=-1 ):#If a solution was found by LP
				#Find new total time
				for j in pairs:#Find cost of each pair
					op1=j[0]
					op2=j[1]
					max=-1
					sum=0
					slowest=-1
					#Find total number of enabled links (communication between devices)
					for dev1 in range(numberOfDevices):
						enabledLinks=0
						for dev2 in range(numberOfDevices):
							if(fractions[op1][dev1]!=0 and fractions[op2][dev2]!=0 and dev1!=dev2):
								enabledLinks+=1
							sum=sum+(fractions[op1][dev1]*pairs[op1,op2]*comCost[dev1][dev2]*fractions[op2][dev2])
						sum=sum+alpha*enabledLinks#Penalty in case of multiple communication links enabled
						if(sum>max):
							max=sum
							slowest=dev1
						sum=0
					transferTimes[(op1,op2)]=round(max,3)
					slowestDevices[
						(op1,op2)]=slowest

				#Find slowest path
				slowestPath=-1
				counter=0
				totalTransferTime=0
				for path in paths:
					transTime=0
					for i in range(len(path)-1):
						transTime=transTime+transferTimes[path[i],path[i+1]]
					if(transTime>totalTransferTime):
						totalTransferTime=transTime
						slowestPath=counter
					counter+=1
		
				#Calculate new maximum DQ fraction possible
				DQfractions={}
				sum=0
				for op in source:
					temp=[]
					for dev in range(numberOfDevices):
						if(available[op][dev]==1 and fractions[op][dev]!=0):
							a=(CCpu[dev]-UsedCpu[dev])/(RCPUDQ*fractions[op][dev])
							b=(CMem[dev]-UsedMem[dev])/(RRAMDQ*fractions[op][dev])
							if(a<b):
								x=a
							else:
								x=b
							if(x>1):
								x=1
							if(x<0):
								x=0
							x=round(x,2)
							UsedCpu[dev]=UsedCpu[dev]+x*fractions[op][dev]*RCPUDQ
							UsedMem[dev]=UsedMem[dev]+x*fractions[op][dev]*RRAMDQ
							temp.append(x)
							sum+=fractions[op][dev]*x
						else:
							temp.append(0)
					DQfractions[op]=temp
				for dev in range(numberOfDevices):
					UsedCpu[dev]=round(UsedCpu[dev],2)
					UsedMem[dev]=round(UsedMem[dev],2)
				DQfraction=1/noOfSources*sum
				
				F=totalTransferTime/(1+beta*DQfraction)
				DQfraction=round(DQfraction,3)
				totalTransferTime=round(totalTransferTime,3)
				F=round(F,3)
				
				#Call optimizer and find best solution
				lpopt=LPOptimizer()
				if(FEQ<F and FEQ!=0):#If equal found a better solution than LP
					#Call optimizer using equal solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpuEQ,UsedMemEQ,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,noOfSources,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,DQfractionEQ,totalTransferTimeEQ,FEQ,beta,alpha,slowestPathEQ,qThreshold)
					if(FSpring<FLat and FSpring<FQual and FSpring!=0):#If Spring Relaxation found a better solution than qualOpt and latOpt the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is solved by Equal assignment
						solvedBy="Equal"
				else:#If LP is better than equal
					#Call optimizer using LP solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,DQfraction,totalTransferTime,F,beta,alpha,slowestPath,qThreshold)
					if(FSpring<FLat and FSpring<FQual and FSpring!=0):#If Spring Relaxation found a better solution than qualOpt and latOpt the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is solved by LP
						solvedBy="LP"
			else:#Id LP found no solution
				if(FEQ!=0):#If Equal found a solution
					#Call optimizer using equal solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpuEQ,UsedMemEQ,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,noOfSources,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,DQfractionEQ,totalTransferTimeEQ,FEQ,beta,alpha,slowestPathEQ,qThreshold)
					if(FSpring<FLat and FSpring<FQual and FSpring!=0):#If Spring Relaxation found a better solution than qualOpt and latOpt the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is solved by Equal assignment
						solvedBy="Equal"
				else:#If Equal found no solution
					FLat=0
					FQual=0
					DQfractionLat=0
					DQfractionQual=0
					totalTransferTimeLat=0
					totalTransferTimeQual=0
					if(FSpring!=0):#If Spring Relaxation found a solution the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is not solved 
						solvedBy="None"
				F=0
				DQfraction=0
				totalTransferTime=0
			
			f = open("F.txt", "a")
			f.write("LP: "+ str(F)+", Spring: "+str(FSpring)+", latOpt: "+ str(FLat)+ ", qualOpt: "+str(FQual)+ ", Equal: " +str(FEQ)+", solved by: "+solvedBy+"\n")
			f.close()
			f = open("DQ.txt", "a")
			f.write("LP: "+ str(DQfraction)+", Spring: "+str(DQfractionSpring)+", latOpt: "+ str(DQfractionLat)+ ", qualOpt: "+str(DQfractionQual)+ ", Equal: " +str(DQfractionEQ)+", solved by: "+solvedBy+"\n")
			f.close()
			f = open("time.txt", "a")
			f.write("LP: "+ str(totalTransferTime)+", Spring: "+str(totalTransferTimeSrping)+", latOpt: "+ str(totalTransferTimeLat)+ ", qualOpt: "+str(totalTransferTimeQual)+ ", Equal: " +str(totalTransferTimeEQ)+", solved by: "+solvedBy+"\n")
			f.close()
			

	
