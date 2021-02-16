#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing the Spring 
#	Relaxation algorithm for task placement.
#-------------------------------------------#

from VivaldyMain import VivaldyMain
import random
from scipy.spatial import distance
import numpy as np
import copy
from HeuristicForSpring import HeuristicForSpring

class SpringRelaxation():
	global closest_executor
	global placement
	def closest_executor(operator, executors):#Finds the closest device to the operator
		closest_index = distance.cdist([operator], executors).argmin()
		return closest_index
	
	def placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag):#Places operators to devices
		if(currExecutorPositions[1:] == currExecutorPositions[:-1]):#No placement available
				flag=1
				return fractions,UsedCpu,UsedMem,flag
		closest=closest_executor(operatorPositions,currExecutorPositions)
		if(RCpu[node]*fraction<=CCpu[closest]-UsedCpu[closest] and RMem[node]*fraction<=CMem[closest]-UsedMem[closest] and available[node][closest]==1):#CPU,MEM sufficiency and available device
			#Assign fraction to device
			fractions[closest]=fraction
			UsedCpu[closest]+=RCpu[node]*fraction
			UsedMem[closest]+=RMem[node]*fraction
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
			(fractions,UsedCpu,UsedMem,flag)=placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag)
		elif(available[node][closest]==0):#Device not available
			#Re-call the procedure to assign the fraction to the next closest device
			currExecutorPositions[closest]=[100000,100000,100000]
			(fractions,UsedCpu,UsedMem,flag)=placement(operatorPositions,currExecutorPositions,node,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,fraction,fractions,flag)
		return fractions,UsedCpu,UsedMem,flag
			
	def run(self,numberOfDevices,comCost,numberOfOperators,paths,pairs,source,children,parents,RCpu,RMem,CCpu,CMem,available
	,tempFractions,RCPUDQ,RRAMDQ,noOfSources,beta,alpha):
		#Run vivaldi algorithm to create cost space for devices
		h=VivaldyMain()
		executorPositions=h.run(numberOfDevices,comCost)
		fractions=[]
		UsedCpu=[]
		UsedMem=[]
		for i in range(numberOfDevices):
			UsedCpu.append(0)
			UsedMem.append(0)
		for i in range(numberOfOperators):
			fractions.append([])
			if(i not in source):
				for j in range(numberOfDevices):
					fractions[i].append(0)
			else:
				fractions[i]=tempFractions[i]
		
		#For each source node
		for op in source:
			for dev in range(numberOfDevices):
				UsedCpu[dev]=UsedCpu[dev]+fractions[op][dev]*RCpu[op]
				UsedMem[dev]=UsedMem[dev]+fractions[op][dev]*RMem[op]
	
		operatorPositions=[]
		for i in range(numberOfOperators):
			if(i not in source):
				operatorPositions.append([random.uniform(-1,1),random.uniform(-1,1),random.uniform(-1,1)])
			else:
				operatorPositions.append([])
				
		global force
		force=[]
		#Place operators on cost space
		for i in range(numberOfOperators):
			if(i not in source):
				force=[0,0,0]
				while True:
					for parent in parents[i]:
						if(parent in source):
							for j in range(3):
								for k in range(numberOfDevices):
									if(fractions[parent][k]>0):
										force[j]=force[j]+(executorPositions[k][j]-operatorPositions[i][j])*pairs[parent,i]#*0.1
						else:
							for j in range(3):
								force[j]=force[j]+(operatorPositions[parent][j]-operatorPositions[i][j])*pairs[parent,i]#*0.1
					for child in children[i]:
						for j in range(3):
							force[j]=force[j]+(operatorPositions[child][j]-operatorPositions[i][j])*pairs[i,child]#*0.1#0.01
					for j in range(3):
						operatorPositions[i][j]=operatorPositions[i][j]+force[j]*0.1
					
					if(np.linalg.norm(np.array(force))<=1):
						break
	
		#Place operators on execcutors
		for i in range(numberOfOperators):
			if(i not in source):
				currExecutorPositions=copy.deepcopy(executorPositions)
				(fractions[i],UsedCpu,UsedMem,flag)=placement(operatorPositions[i],currExecutorPositions,i,RCpu,CCpu,RMem,CMem,UsedCpu,UsedMem,available,1,fractions[i],0)
				
				if(flag==1):
					break;
		if(flag==1):#No placement found
			return(0,0,0)
		
		else:
			transferTimes={}
			slowestDevices={}
			#Find new total time
			for j in pairs:#Find cost of each pair
				op1=j[0]
				op2=j[1]
				max=-1
				sum=0
				slowest=-1
				#Find total number of enabled links (communication between devices)
				for dev1 in range(numberOfDevices):#Find total cost
					enabledLinks = 0
					for dev2 in range(numberOfDevices):
						if (fractions[op1][dev1] != 0 and fractions[op2][dev2] != 0 and dev1 != dev2):
							enabledLinks += 1
						sum = sum + (fractions[op1][dev1] * pairs[op1, op2] * comCost[dev1][dev2] * fractions[op2][dev2])
					sum = sum + alpha * enabledLinks#Penalty in case of multiple communication links enabled
					if(sum>max):
						max=sum
						slowest=dev1
					sum=0
				transferTimes[(op1,op2)]=round(max,3)
				slowestDevices[(op1,op2)]=slowest
				
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
			if (DQfraction > 1):  # Due to float operations
				DQfraction = 1
			F=totalTransferTime/(1+beta*DQfraction)
			DQfractionSpring=round(DQfraction,3)
			totalTransferTimeSrping=round(totalTransferTime,3)
			FSpring=round(F,3)
			
			#Run heuristics
			hs=HeuristicForSpring()



			(DQfractionSpringH,totalTransferTimeSrpingH,FSpringH)=hs.run(numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs
	,paths,source,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,F,beta,alpha,executorPositions,operatorPositions)
			if(FSpringH<0 or FSpring<0):#If no solution was found
				return(0,0,0)
			elif(FSpringH!=0):#If the heuristics solution is better
				return(DQfractionSpringH,totalTransferTimeSrpingH,FSpringH)
			else:#Return the initial Spring Relaxation solution
				return(DQfractionSpring,totalTransferTimeSrping,FSpring)
			
		
	
