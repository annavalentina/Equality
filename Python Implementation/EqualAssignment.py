#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class that equally distributes
#	tasks to all avaialable devices.
#-------------------------------------------#
from pulp import *
import numpy as np
import copy
import random 
import time

class EqualAssignment():
	def findMetrics(self,numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source,fractions,UsedCpu,UsedMem):
		#Find new total time
		transferTimes={}
		slowestDevices={}
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
					UsedCpu[dev]=UsedCpu[dev]+x*RCPUDQ*fractions[op][dev]
					UsedMem[dev]=UsedMem[dev]+x*RRAMDQ*fractions[op][dev]
					temp.append(x)
					sum+=fractions[op][dev]*x
				else:
					temp.append(0)
			DQfractions[op]=temp
		DQfraction=(1/noOfSources)*sum		
		F=totalTransferTime/(1+beta*DQfraction)
		DQfraction=round(DQfraction,3)
		totalTransferTime=round(totalTransferTime,3)
		F=round(F,3)
		return(F,totalTransferTime,DQfraction,UsedCpu,UsedMem,fractions,DQfractions,transferTimes,slowestDevices,slowestPath)
		

	def assignment(self,op,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,fraction,availableDevices,UsedCpu,UsedMem,fractions,notAvailableDevices):
		flag=0
		equalFraction=fraction/availableDevices#Find what fraction to assign to each device
		remainingFraction=0
		for dev in range(numberOfDevices):
			if(dev not in notAvailableDevices):#If device is available
				if((UsedCpu[dev]+equalFraction*RCpu[op])<=CCpu[dev] and (UsedMem[dev]+equalFraction*RMem[op]) <= CMem[dev]):#CPU, MEM sufficiency
					fractions[op][dev]=round(fractions[op][dev]+equalFraction,3)
					UsedCpu[dev]=round(UsedCpu[dev]+equalFraction*RCpu[op],3)
					UsedMem[dev]=round(UsedMem[dev]+equalFraction*RMem[op],3)
				else:#CPU, MEM inefficiency
					memDiff=(RMem[op]*equalFraction-(CMem[dev]-UsedMem[dev]))/(RMem[op]*equalFraction)
					cpuDiff=(RCpu[op]*equalFraction-(CCpu[dev]-UsedCpu[dev]))/(RCpu[op]*equalFraction)
					#Assign as much fraction as possible and keep the remaining fraction and set the device as not available
					newfraction=0
					if(memDiff>cpuDiff):
						if(memDiff>0):
							newfraction=memDiff*equalFraction
					else:
						if(cpuDiff>0):
							newfraction=cpuDiff*equalFraction
					if(newfraction!=0):
						availableDevices-=1
						fractions[op][dev]=round(fractions[op][dev]+equalFraction-newfraction,3)
						UsedCpu[dev]=round(UsedCpu[dev]+(equalFraction-newfraction)*RCpu[op],3)
						UsedMem[dev]=round(UsedMem[dev]+(equalFraction-newfraction)*RMem[op],3)
						remainingFraction+=newfraction
						notAvailableDevices.append(dev)
					else:
						availableDevices-=1
						notAvailableDevices.append(dev)
		
		if(remainingFraction>0):#If fraction remains re-call the procedure to assign it to the devices
			if(availableDevices>0):	
				(fractions,UsedCpu,UsedMem,flag)=self.assignment(op,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,remainingFraction,availableDevices,UsedCpu,UsedMem,fractions,notAvailableDevices)
			else:#If no available devices remain
				return (fractions,UsedCpu,UsedMem,1)
		return (fractions,UsedCpu,UsedMem,flag)
	
	def run(self,numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
	,paths,source,noOfSources,tempFractions,beta,alpha):
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

		for op in range(numberOfOperators):
			#For each operator create list with not available devices
			if(op not in source):
				notAvailableDevices=[]
				availableDevices=0
				for dev in range(numberOfDevices):
					if(available[op][dev]==1):
						availableDevices+=1
					else:
						notAvailableDevices.append(dev)
				#Assign the operators equally to all devices
				(fractions,UsedCpu,UsedMem,flag)=self.assignment(op,numberOfOperators,pairs,comCost,numberOfDevices,available,RCpu,CCpu,RMem,CMem,parents,1,availableDevices,UsedCpu,UsedMem,fractions,notAvailableDevices)
				if(flag==1):
					break
		
		if(flag==1):#If no solution was found
			return(0,0,0,0,0,0,0,0,0,0)
		else:
			(F,totalTransferTime,DQfraction,UsedCpu,UsedMem,fractions,DQfractions,transferTimes,slowestDevices,slowestPath)=self.findMetrics(numberOfDevices,comCost,paths,pairs,noOfSources,beta,alpha,available,RCPUDQ,RRAMDQ,CCpu,CMem,source,fractions,UsedCpu,UsedMem)
			return(DQfraction,totalTransferTime,F,UsedCpu,UsedMem,fractions,transferTimes,slowestDevices,DQfractions,slowestPath)

