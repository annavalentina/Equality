#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing greedy 
#	algorithms for optimizing an initial 
#	task placement solution based on 
#	latency or quality.
#-------------------------------------------#

import numpy as np
import copy
import gurobipy as gp
from gurobipy import GRB
from FindMetrics import findMetricsFun

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
				model = gp.Model("lp")
				model.Params.OutputFlag = 0
				transferTime =model.addVar(vtype=GRB.CONTINUOUS, lb=0,name="transferTime")
				fList = list(range(numberOfDevices))
				fraction = model.addVars(fList, lb=0, ub=100, vtype=GRB.INTEGER, name="x")
				binZerol = list(range(numberOfDevices))
				binSelfl = list(range(numberOfDevices))
				binZero = model.addVars(binZerol, lb=0, ub=1, vtype=GRB.INTEGER, name="bz")#0 if the destination device will not hold data, 1 otherwise
				binSelf = model.addVars(binSelfl, lb=0, ub=1, vtype=GRB.INTEGER, name="bs")#1 if the destination device is the same as the starting one, 0 otherwise

				c1 = 0
				for i in range(numberOfDevices):#CPU, RAM constraints
					model.addConstr((fraction[i] * RCpu[op2] / 100) <= (CCpu[i] - tempUsedCpu[i] + tempFractions[op2][i] * RCpu[op2]))
					model.addConstr((fraction[i] * RMem[op2] / 100) <= (CMem[i] - tempUsedMem[i] + tempFractions[op2][i] * RMem[op2]))
					if(available[op2][i]==0):#Sum of fractions equal to 100
						model.addConstr(fraction[i]==0)
					c1+=fraction[i]#Sum of fractions equal to 100
					model.addConstr(binSelf[i] >= fraction[i] / 100)
					model.addConstr(binSelf[i] <= fraction[i] / 100 + 0.99)
					model.addConstr(binZero[i] >= fraction[i] / 100)
					model.addConstr(binZero[i] <= fraction[i] / 100 + 0.99)
				model.addConstr(c1==100)
				

				for op1 in parents[op2]:
					for dev1 in range(numberOfDevices):
						c2 = 0
						for dev2 in range(numberOfDevices):
							c2=c2+(tempFractions[op1][dev1]*pairs[op1,op2]*comCost[dev1][dev2]*fraction[dev2])/100
						if (tempFractions[op1][dev1] != 0):#If the device holds data
							c2 = c2 + alpha * (binZero.sum() - binSelf[dev1])#Penalty for each enabled link to other device
						model.addConstr(transferTime >= c2)#Minimize the max
				model.setObjective(transferTime, GRB.MINIMIZE)
				model.optimize()
				if (model.STATUS != GRB.OPTIMAL):
					solved = -1
				else:
					solved = 1
				if (solved == -1):
					flag = 1  # No solution
				if (solved != -1):
				#Enforce solution
					for i in range(numberOfDevices):
						tempUsedCpu[i]=tempUsedCpu[i]+(fraction[i].x*RCpu[op2]/100)-tempFractions[op2][i]*RCpu[op2]
						tempUsedMem[i]=tempUsedMem[i]+(fraction[i].x*RMem[op2]/100)-tempFractions[op2][i]*RMem[op2]
						tempFractions[op2][i]=round((fraction[i].x)/100,3)
						
				#Call the function for the children of the operator
				self.recursivePairsLP(op2,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,flag,alpha)
		return flag
		


	#Optimize based on latency
	def latOpt(self,numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,
			   beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,sourceChild,DQfractions,
			   DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,DQMode):
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
			#for op in source:
			for op in DQfractions:
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
				tempFractions[op1][slowestDevice]=round(tempFractions[op1][slowestDevice]-betaF,3)	#Remove fraction from bottleneck
				tempUsedCpu[slowestDevice]=round(tempUsedCpu[slowestDevice]-betaF*RCpu[op1],2)
				tempUsedMem[slowestDevice]=round(tempUsedMem[slowestDevice]-betaF*RMem[op1],2)
				
				for k in range(numberOfDevices):#For all the other devices
					if (fractions[op1][k]!=0 and k!=slowestDevice):
						#If the device can handle more data
						if( (tempUsedCpu[k]+dividedFraction*RCpu[op1]) <=CCpu[k] 
						and (tempUsedMem[k]+dividedFraction*RMem[op1]) <=CMem[k]):
							tempFractions[op1][k]=round(tempFractions[op1][k]+dividedFraction,3)
							tempUsedCpu[k]=round(tempUsedCpu[k]+dividedFraction*RCpu[op1],2)
							tempUsedMem[k]=round(tempUsedMem[k]+dividedFraction*RMem[op1],2)
						#If not the bottleneck device keeps them
						else:
							tempFractions[op1][slowestDevice]=round(tempFractions[op1][slowestDevice]+dividedFraction,3)
							tempUsedCpu[slowestDevice]=round(tempUsedCpu[slowestDevice]+dividedFraction*RCpu[op1],2)
							tempUsedMem[slowestDevice]=round(tempUsedMem[slowestDevice]+dividedFraction*RMem[op1],2)
				#Enforce changes downstream the DAG and calculate metrics
				flag=self.recursivePairsLP(op1,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,0,alpha)

				(tempF,temptotalTransferTime,tempDQfraction,TempDQfractions,tempUsedCpu,tempUsedMem,tempTransferTimes,tempSlowestDevices,tempSlowestPath)=\
					findMetricsFun(numberOfDevices, comCost, paths, pairs, noOfSources, beta, alpha, available, RCPUDQ,
								   RRAMDQ, CCpu,CMem, source, sourceChild, tempFractions, tempUsedCpu, tempUsedMem,DQMode)

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
	def qualOpt(self,numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,
				beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,sourceChild,DQfractions,
				DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,qThreshold,DQMode):
		iterflag=1
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		#for op in source:

		for op in DQfractions:
			tempTransferTimes=copy.deepcopy(transferTimes)
			tempFractions=copy.deepcopy(fractions)
			tempUsedCpu=copy.deepcopy(UsedCpu)
			tempUsedMem=copy.deepcopy(UsedMem)
			tempSlowestDevices=copy.deepcopy(slowestDevices)
			#for op in source:
			for op in DQfractions:
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
								tempFractions[i][dev]=round(tempFractions[i][dev]-betaF,3)	#Remove fraction from source device
								tempUsedCpu[dev]=round(tempUsedCpu[dev]-betaF*RCpu[i],2)
								tempUsedMem[dev]=round(tempUsedMem[dev]-betaF*RMem[i],2)
			
								for k in range(numberOfDevices):
									if (fractions[i][k]!=0 and k!=dev):#For all the other devices
									#If the device can handle more data
										if( (tempUsedCpu[k]+dividedFraction*RCpu[i]) <=CCpu[k] 
										and (tempUsedMem[k]+dividedFraction*RMem[i]) <=CMem[k]):
											tempFractions[i][k]=round(tempFractions[i][k]+dividedFraction,3)
											tempUsedCpu[k]=round(tempUsedCpu[k]+dividedFraction*RCpu[i],2)
											tempUsedMem[k]=round(tempUsedMem[k]+dividedFraction*RMem[i],2)
										#If not the source device keeps them
										else:
											tempFractions[i][dev]=round(tempFractions[i][dev]+dividedFraction,3)
											tempUsedCpu[dev]=round(tempUsedCpu[dev]+dividedFraction*RCpu[i],2)
											tempUsedMem[dev]=round(tempUsedMem[dev]+dividedFraction*RMem[i],2)
								#Enforce changes downstream the DAG and calculate metrics
								flag=self.recursivePairsLP(i,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,0,alpha)
								if(flag==0):
									(tempF, temptotalTransferTime, tempDQfraction, TempDQfractions, tempUsedCpu, tempUsedMem,tempTransferTimes, tempSlowestDevices, tempSlowestPath) = \
										findMetricsFun(numberOfDevices,comCost, paths, pairs,noOfSources, beta,alpha, available,
												RCPUDQ, RRAMDQ,CCpu,CMem, source,sourceChild,tempFractions,
												tempUsedCpu,tempUsedMem,DQMode)

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


	def run(self,numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
	,paths,source,sourceChild,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,DQfraction,totalTransferTime,F,beta,alpha,slowestPath,qThreshold,DQMode):
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices

		iterflag=0
		for iterator in range(10):
			if(iterflag==0):
				(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)=self.latOpt(numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,sourceChild,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,DQMode)
				totalTransferTimePairs=totalTransferTime
				DQfractionPairs=DQfraction
				FPairs=F
			else:#If no solution is found break
				break;
		iterflag=0
		for iterator in range(10):
			if(iterflag==0):
				(transferTimes,totalTransferTime,fractions,slowestDevices,slowestPath,UsedCpu,UsedMem,DQfractions,DQfraction,F,iterflag)=self.qualOpt(numberOfOperators,pairs,paths,comCost,numberOfDevices,noOfSources,available,RCpu,CCpu,RMem,CMem,beta,alpha,parents,transferTimes,fractions,UsedCpu,UsedMem,slowestDevices,F,source,sourceChild,DQfractions,DQfraction,RRAMDQ,RCPUDQ,totalTransferTime,slowestPath,qThreshold,DQMode)
				totalTransferTimeDQ = totalTransferTime
				DQfractionDQ=DQfraction
				FDQ=F
			else:#If no solution is found break
				break;

		return(DQfractionPairs,totalTransferTimePairs,FPairs,DQfractionDQ,totalTransferTimeDQ,FDQ)#If no better solution is found it returns the initial one

