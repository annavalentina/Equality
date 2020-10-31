#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing a greedy
#	algorithm for optimizing an initial 
#	Spring Relaxation task placement 
#	solution based on cost space.
#-------------------------------------------#
from pulp import *
import numpy as np
from scipy.spatial import distance
import random 
import copy
class HeuristicForSpring():
	global tempFractions
	global tempTransferTimes
	global tempUsedCpu
	global tempUsedMem
	global closest_executor
	global placement
	
	def closest_executor(operator, executors):#Finds the closest device to the operator
		closest_index = distance.cdist([operator], executors).argmin()
		return closest_index
	
	def placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag,fractionToAssign,slowestDevice):#Places operators to devices
		if(currExecutorPositions[1:] == currExecutorPositions[:-1]):#No placement available
				flag=1
				return fractions,UsedCpu,UsedMem,flag
		closest=closest_executor(operatorPositions,currExecutorPositions)
		if(closest==slowestDevice):#If the closest device is also the slowest assign to it 0.7*previous_fraction
			fraction=round(fractionToAssign,2)
		if(RCpu[node]*fraction<=CCpu[closest]-UsedCpu[closest] and RMem[node]*fraction<=CMem[closest]-UsedMem[closest] and available[node][closest]==1):#CPU,MEM sufficiency and available device
			#Assign fraction to device
			fractions[closest]=fraction
			UsedCpu[closest]+=RCpu[node]*fraction
			UsedMem[closest]+=RMem[node]*fraction
			if(closest==slowestDevice):#If the closest device is also the slowest re-call the procedure with new fraction 0.3*previous_fraction to assign to the second closest device
				currExecutorPositions[closest]=[100000,100000,100000]
				(fractions,UsedCpu,UsedMem,flag)=placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,round(1-fraction,2),fractions,flag,fractionToAssign,slowestDevice)
			else:
				return fractions,UsedCpu,UsedMem,flag
		elif((RCpu[node]*fraction>(CCpu[closest]-UsedCpu[closest]) or RMem[node]*fraction>(CMem[closest]-UsedMem[closest])) and available[node][closest]==1):#CPU or MEM inefficiency
			#Assign as much fraction as possible and re-call the procedure to assign the rest of the fraction to the next closest device
			currExecutorPositions[closest]=[100000,100000,100000]
			memDiff=(RMem[node]*fraction-(CMem[closest]-UsedMem[closest]))/(RMem[node]*fraction)
			cpuDiff=(RCpu[node]*fraction-(CCpu[closest]-UsedCpu[closest]))/(RCpu[node]*fraction)
			prevFraction=fraction
			if(memDiff>cpuDiff):
				fraction=memDiff*prevFraction
			else:
				fraction=cpuDiff*prevFraction
			fractions[closest]=prevFraction-fraction
			UsedCpu[closest]+=RCpu[node]*(prevFraction-fraction)
			UsedMem[closest]+=RMem[node]*(prevFraction-fraction)
			(fractions,UsedCpu,UsedMem,flag)=placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag,fractionToAssign,slowestDevice)
		elif(available[node][closest]==0):#Device not available
			#Re-call the procedure to assign the fraction to the next closest device
			currExecutorPositions[closest]=[100000,100000,100000]
			(fractions,UsedCpu,UsedMem,flag)=placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag,fractionToAssign,slowestDevice)
		return fractions,UsedCpu,UsedMem,flag
		
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
	,paths,source,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,DQfraction,totalTransferTime,F,beta,alpha,slowestPath,executorPositions,operatorPositions):
		global tempFractions
		global tempTransferTimes
		global tempUsedCpu
		global tempUsedMem
		global tempSlowestDevices
		
		tempTransferTimes=copy.deepcopy(transferTimes)
		tempSlowestDevices=copy.deepcopy(slowestDevices)
		bottleneckOperator=-1
		maxCost=-1
		slowestDevice=-1
	
		#Find bottleneck operator and slowest device
		for j in pairs:
			if(transferTimes[(j[0],j[1])]>maxCost and j[0] not in source and transferTimes[(j[0],j[1])]>0 ):
				maxCost=transferTimes[(j[0],j[1])]
				bottleneckOperator=j[0]
				slowestDevice=slowestDevices[(j[0],j[1])]
		if(bottleneckOperator!=-1):
			tempExecutorPositions=copy.deepcopy(executorPositions)
			tempUsedCpu=copy.deepcopy(UsedCpu)
			tempUsedMem=copy.deepcopy(UsedMem)
			
			#Free resources on devices hosting the bottleneck operator  
			for i in range(numberOfDevices):
				tempUsedCpu[i]=tempUsedCpu[i]-fractions[bottleneckOperator][i]*RCpu[bottleneckOperator]
				tempUsedMem[i]=tempUsedMem[i]-fractions[bottleneckOperator][i]*RMem[bottleneckOperator]
			tempFractions=copy.deepcopy(fractions)
			#Remove DQ fraction from sources in order to re-decide it based on new placement of the bottleneck operator
			for op in source:
				for dev in range(numberOfDevices):
					tempUsedMem[dev]-=DQfractions[op][dev]*RRAMDQ*fractions[op][dev]
					tempUsedCpu[dev]-=DQfractions[op][dev]*RCPUDQ*fractions[op][dev]
			#Slowest device will keep 70% of its previous bottleneck operator's fraction 
			fractionToAssign=fractions[bottleneckOperator][slowestDevice]-fractions[bottleneckOperator][slowestDevice]*0.3
			#Place bottleneck operator
			(tempFractions[bottleneckOperator],tempUsedCpu,tempUsedMem,flag)=placement(operatorPositions[bottleneckOperator],tempExecutorPositions,bottleneckOperator,RCpu,CCpu,RMem,CMem,tempUsedCpu,tempUsedMem,available,1,tempFractions[bottleneckOperator],0,fractionToAssign,slowestDevice)
			(tempF,temptotalTransferTime,tempSlowestPath,TempDQfractions,tempDQfraction)=self.findMetrics(numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source)#Find new metrics
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
				DQfraction=round(DQfraction,3)
				totalTransferTime=round(totalTransferTime,3)
				F=round(F,3)
				return(DQfraction,totalTransferTime,F)
			else:#If no better solution was found
				return(0,0,0)


