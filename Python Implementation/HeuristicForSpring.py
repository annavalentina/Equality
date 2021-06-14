#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing a greedy
#	algorithm for optimizing an initial 
#	Spring Relaxation task placement 
#	solution based on cost space.
#-------------------------------------------#

from scipy.spatial import distance
import copy
from FindMetrics import findMetricsFun

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

	def run(self,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs
	,paths,source,sourceChild,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,F,beta,alpha,executorPositions,operatorPositions,DQMode):
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
			#for op in source:
			for op in DQfractions:
				for dev in range(numberOfDevices):
					tempUsedMem[dev]-=DQfractions[op][dev]*RRAMDQ*fractions[op][dev]
					tempUsedCpu[dev]-=DQfractions[op][dev]*RCPUDQ*fractions[op][dev]
			#Slowest device will keep 70% of its previous bottleneck operator's fraction 
			fractionToAssign=fractions[bottleneckOperator][slowestDevice]-fractions[bottleneckOperator][slowestDevice]*0.3
			#Place bottleneck operator
			(tempFractions[bottleneckOperator],tempUsedCpu,tempUsedMem,flag)=placement(operatorPositions[bottleneckOperator],tempExecutorPositions,bottleneckOperator,RCpu,CCpu,RMem,CMem,tempUsedCpu,tempUsedMem,available,1,tempFractions[bottleneckOperator],0,fractionToAssign,slowestDevice)
			(tempF, temptotalTransferTime, tempDQfraction, TempDQfractions, tempUsedCpu, tempUsedMem, tempTransferTimes,
			 tempSlowestDevices, tempSlowestPath) = findMetricsFun(numberOfDevices, comCost, paths, pairs, noOfSources, beta,
															alpha, available, RCPUDQ, RRAMDQ,
															CCpu,
															CMem, source, sourceChild, tempFractions,
															tempUsedCpu, tempUsedMem,DQMode)

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
				return(DQfraction,totalTransferTime,F)
			else:#If no better solution was found
				return(0,0,0)


