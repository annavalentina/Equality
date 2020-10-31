#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing greedy 
#	algorithms for optimizing an initial 
#	task placement solution based on 
#	latency or quality.
#-------------------------------------------#
from pulp import *
import numpy as np
import copy
import random 
import time

class LPOptimizer():
	global tempFractions
	global tempTransferTimes
	global tempUsedCpu
	global tempUsedMem
	
	
	def recursivePairsLP(self,operator,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,flag,alpha):
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		
		for op2 in range(numberOfOperators):
			if(operator in parents[op2]):
				problem = LpProblem("problemName", LpMinimize)
				executionTime = LpVariable("transferTime")
				fraction = LpVariable.dicts("x", list(range(numberOfDevices)), 0, 100, cat="Integer")

				for i in range(numberOfDevices):#Constraints
					problem+=(fraction[i]*RCpu[op2]/100)<=(CCpu[i]-tempUsedCpu[i]+tempFractions[op2][i]*RCpu[op2])
					problem+=(fraction[i]*RMem[op2]/100)<=(CMem[i]-tempUsedMem[i]+tempFractions[op2][i]*RMem[op2])
					if(available[op2][i]==0):
						problem+=fraction[i]==0

				c1=0
				for i in range(numberOfDevices):
					c1+=fraction[i]#Sum of fractions equal to 100
				problem+=c1==100
				
				c2=0
				for op1 in parents[op2]:
					for dev1 in range(numberOfDevices):
						for dev2 in range(numberOfDevices):
							c2=c2+((tempFractions[op1][dev1])*pairs[op1,op2]*comCost[dev1][dev2]*fraction[dev2])/100						
					
				transferTime=c2
				problem+=transferTime
				solved=problem.solve()#Solve LP
				if(solved==-1):
					flag=1#No solution
				#Enforce solution
				for i in range(numberOfDevices):
					tempUsedCpu[i]=tempUsedCpu[i]+(fraction[i].varValue*RCpu[op2]/100)-tempFractions[op2][i]*RCpu[op2]
					tempUsedMem[i]=tempUsedMem[i]+(fraction[i].varValue*RMem[op2]/100)-tempFractions[op2][i]*RMem[op2]
					tempFractions[op2][i]=round(fraction[i].varValue)/100
						
				#Call the function for the children of the operator
				self.recursivePairsLP(op2,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,flag,alpha)
		return flag
		


	#Optimize based on latency
	def latOpt(self,numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath):
		iterflag=1		
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		global solved
		for j in pairs:
			tempTransferTimes=copy.deepcopy(transferTimes)
			tempFractions=copy.deepcopy(fractions)
			tempUsedCpu=copy.deepcopy(UsedCpu)
			tempUsedMem=copy.deepcopy(UsedMem)
			tempSlowestDevices=copy.deepcopy(slowestDevices)
			for op in source:
				for dev in range(numberOfDevices):
					tempUsedMem[dev]-=DQfractions[op][dev]*RRAMDQ*fractions[op][dev]
					tempUsedCpu[dev]-=DQfractions[op][dev]*RCPUDQ*fractions[op][dev]
			op1=j[0]
			op2=j[1]
			availableDevices=len((np.nonzero(fractions[op1]))[0])
			if(availableDevices!=1 and op1 not in source and availableDevices!=0):#If more than one devices hold data	
				slowestDevice=slowestDevices[op1,op2]#Find bottleneck/slowest device
				betaF=tempFractions[op1][slowestDevice]*0.3#Fraction to remove from bottleneck device (30%)
				dividedFraction=betaF/(availableDevices-1)#Fraction to divide to other devices that aleady hold data
				tempFractions[op1][slowestDevice]=round(tempFractions[op1][slowestDevice]-betaF,2)	#Remove fraction from bottleneck
				tempUsedCpu[slowestDevice]=round(tempUsedCpu[slowestDevice]-betaF*RCpu[op1],2)
				tempUsedMem[slowestDevice]=round(tempUsedMem[slowestDevice]-betaF*RMem[op1],2)
				
				for k in range(numberOfDevices):#For all the other devices
					if (fractions[op1][k]!=0 and k!=slowestDevice):
						#If the device can handle more data
						if( (tempUsedCpu[k]+dividedFraction*RCpu[op1]) <=CCpu[k] 
						and (tempUsedMem[k]+dividedFraction*RMem[op1]) <=CMem[k]):
							tempFractions[op1][k]=round(fractions[op1][k]+dividedFraction,2)
							tempUsedCpu[k]=round(tempUsedCpu[k]+dividedFraction*RCpu[op1],2)
							tempUsedMem[k]=round(tempUsedMem[k]+dividedFraction*RMem[op1],2)
						#If not the bottleneck device keeps them
						else:
							tempFractions[op1][slowestDevice]=round(tempFractions[op1][slowestDevice]+dividedFraction,2)
							tempUsedCpu[slowestDevice]=round(tempUsedCpu[slowestDevice]+dividedFraction*RCpu[op1],2)
							tempUsedMem[slowestDevice]=round(tempUsedMem[slowestDevice]+dividedFraction*RMem[op1],2)
				#Enforce changes downstream the DAG
				flag=self.recursivePairsLP(op1,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,0,alpha)
				(tempF,temptotalTransferTime,tempSlowestPath,TempDQfractions,tempDQfraction)=self.findMetrics(numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source)
				if(tempF<F and flag==0):#If the solution is beneficial
					transferTimes=copy.deepcopy(tempTransferTimes)
					totalTransferTime=temptotalTransferTime
					fractions=copy.deepcopy(tempFractions)
					slowestDevices=copy.deepcopy(tempSlowestDevices)
					slowestPath=tempSlowestPath
					UsedCpu=copy.deepcopy(tempUsedCpu)
					UsedMem=copy.deepcopy(tempUsedMem)
					DQfractions=copy.deepcopy(TempDQfractions)
					DQfraction=tempDQfraction
					F=tempF
					iterflag=0
		return(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)
		
	#Optimize based on quality
	def qualOpt(self,numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,qThreshold):
		iterflag=1
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		for op in source:
			tempTransferTimes=copy.deepcopy(transferTimes)
			tempFractions=copy.deepcopy(fractions)
			tempUsedCpu=copy.deepcopy(UsedCpu)
			tempUsedMem=copy.deepcopy(UsedMem)
			tempSlowestDevices=copy.deepcopy(slowestDevices)
			for op in source:
				for dev in range(numberOfDevices):
					tempUsedMem[dev]-=DQfractions[op][dev]*RRAMDQ*fractions[op][dev]
					tempUsedCpu[dev]-=DQfractions[op][dev]*RCPUDQ*fractions[op][dev]
			for dev in range(numberOfDevices):
				if(tempFractions[op][dev]>0 and (tempUsedCpu[dev]/CCpu[dev]>qThreshold or tempUsedMem[dev]/CMem[dev]>qThreshold)):#If the device is a source and CPU or MEM exceed the threshold 
					for i in range(numberOfOperators):
						if(i not in source):
							availableDevices=len((np.nonzero(fractions[i]))[0])
							if(fractions[i][dev]!=0 and availableDevices!=1 and availableDevices!=0):#If the source device takes on fraction from the operator and more than one devices take on fraction of the operator	
								betaF=tempFractions[i][dev]*0.3#Fraction to remove from source device(30%)
								dividedFraction=betaF/(availableDevices-1)#Fraction to divide to other devices that aleady hold data
								tempFractions[i][dev]=round(tempFractions[i][dev]-betaF,2)	#Remove fraction from source device
								tempUsedCpu[dev]=round(tempUsedCpu[dev]-betaF*RCpu[i],2)
								tempUsedMem[dev]=round(tempUsedMem[dev]-betaF*RMem[i],2)
			
								for k in range(numberOfDevices):
									if (fractions[i][k]!=0 and k!=dev):#For all the other devices
									#If the device can handle more data
										if( (tempUsedCpu[k]+dividedFraction*RCpu[i]) <=CCpu[k] 
										and (tempUsedMem[k]+dividedFraction*RMem[i]) <=CMem[k]):
											tempFractions[i][k]=round(fractions[i][k]+dividedFraction,2)
											tempUsedCpu[k]=round(tempUsedCpu[k]+dividedFraction*RCpu[i],2)
											tempUsedMem[k]=round(tempUsedMem[k]+dividedFraction*RMem[i],2)
										#If not the source device keeps them
										else:
											tempFractions[i][dev]=round(tempFractions[i][dev]+dividedFraction,2)
											tempUsedCpu[dev]=round(tempUsedCpu[dev]+dividedFraction*RCpu[i],2)
											tempUsedMem[dev]=round(tempUsedMem[dev]+dividedFraction*RMem[i],2)
								#Enforce changes downstream the DAG
								flag=self.recursivePairsLP(i,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,0,alpha)
								(tempF,temptotalTransferTime,tempSlowestPath,TempDQfractions,tempDQfraction)=self.findMetrics(numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source)
								if(tempF<F and flag==0):#If the solution is beneficial
									transferTimes=copy.deepcopy(tempTransferTimes)
									totalTransferTime=temptotalTransferTime
									fractions=copy.deepcopy(tempFractions)
									slowestDevices=copy.deepcopy(tempSlowestDevices)
									slowestPath=tempSlowestPath
									UsedCpu=copy.deepcopy(tempUsedCpu)
									UsedMem=copy.deepcopy(tempUsedMem)
									DQfractions=copy.deepcopy(TempDQfractions)
									DQfraction=tempDQfraction
									F=tempF
									iterflag=0
		return(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)
		
		
	def findMetrics(self,numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source):
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		#Find new total time
		for j in pairs:#Find cost of each pair
			op1=j[0]
			op2=j[1]
			max=-1
			sum=0
			slowest=-1
			#Find total number of enabled links (communication between devices)
			for dev1 in range(numberOfDevices):
				enabledLinks = 0
				for dev2 in range(numberOfDevices):
					if (tempFractions[op1][dev1] != 0 and tempFractions[op2][dev2] != 0 and dev1 != dev2):
						enabledLinks += 1
					sum = sum + (tempFractions[op1][dev1] * pairs[op1, op2] * comCost[dev1][dev2] * tempFractions[op2][dev2])
				sum = sum + alpha * enabledLinks#Penalty in case of multiple communication links enabled
				if(sum>max):
					max=sum
					slowest=dev1
				sum=0
			tempTransferTimes[(op1,op2)]=round(max,3)
			tempSlowestDevices[(op1,op2)]=slowest
			
		#Find slowest path
		tempSlowestPath=-1
		counter=0
		temptotalTransferTime=0
		for path in paths:	
			transTime=0
			for i in range(len(path)-1):
				transTime=transTime+tempTransferTimes[path[i],path[i+1]]
			if(transTime>temptotalTransferTime):
				temptotalTransferTime=transTime
				tempSlowestPath=counter
			counter+=1
			
		#Calculate new maximum DQ fraction possible
		tempDQfractions={}
		sum=0
		for op in source:
			temp=[]
			for dev in range(numberOfDevices):
				if(available[op][dev]==1 and tempFractions[op][dev]!=0):
					a=(CCpu[dev]-tempUsedCpu[dev])/(RCPUDQ*tempFractions[op][dev])
					b=(CMem[dev]-tempUsedMem[dev])/(RRAMDQ*tempFractions[op][dev])
								
					if(a<b):
						x=a
					else:
						x=b
					if(x>1):
						x=1
					if(x<0):
						x=0
					x=round(x,2)
					tempUsedCpu[dev]=tempUsedCpu[dev]+x*RCPUDQ*tempFractions[op][dev]
					tempUsedMem[dev]=tempUsedMem[dev]+x*RRAMDQ*tempFractions[op][dev]
					temp.append(x)
					sum+=tempFractions[op][dev]*x
				else:
					temp.append(0)
			tempDQfractions[op]=temp
		tempDQfraction=(1/noOfSources)*sum		
		tempF=temptotalTransferTime/(1+beta*tempDQfraction)
		return(tempF,temptotalTransferTime,tempSlowestPath,tempDQfractions,tempDQfraction)
	
	def run(self,numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
	,paths,source,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,DQfraction,totalTransferTime,F,beta,alpha,slowestPath,qThreshold):
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		iterflag=0
		for iterator in range(10):
			if(iterflag==0):
				(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)=self.latOpt(numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath)		
				DQfractionPairs=round(DQfraction,3)
				totalTransferTimePairs=round(totalTransferTime,3)
				DQfractionDQ=round(DQfraction,3)
				FPairs=round(F,3)
			else:#If no solution is found break
				break;
		iterflag=0
		for iterator in range(10):
			if(iterflag==0):
				(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)=self.qualOpt(numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,qThreshold)
				DQfractionDQ=round(DQfraction,3)
				totalTransferTimeDQ=round(totalTransferTime,3)
				FDQ=round(F,3)
			else:#If no solution is found break
				break;
		return(DQfractionPairs,totalTransferTimePairs,FPairs,DQfractionDQ,totalTransferTimeDQ,FDQ)#If no better solution is find it returns the initial one

