#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class that equally distributes
#	tasks to all avaialable devices.
#-------------------------------------------#

import copy
from FindMetrics import findMetricsFun

class EqualAssignment():
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
	,paths,source,sourceChild,noOfSources,tempFractions,beta,alpha,DQMode):
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
			(F, totalTransferTime, DQfraction, DQfractions, UsedCpu, UsedMem, transferTimes,
			 slowestDevices, slowestPath) = findMetricsFun(numberOfDevices, comCost, paths, pairs, noOfSources,
														  beta,
														  alpha, available, RCPUDQ, RRAMDQ,
														  CCpu,
														  CMem, source, sourceChild, fractions,
														  UsedCpu, UsedMem,DQMode)
			return(DQfraction,totalTransferTime,F,UsedCpu,UsedMem,fractions,transferTimes,slowestDevices,DQfractions,slowestPath)

