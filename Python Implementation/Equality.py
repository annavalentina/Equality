#-------------------------------------------#
#Author: Michailidou Anna-Valentini
#Description:Class implementing a hybrid 
#	solution that finds the trade-off 
#	between latency and data quality check 
#	in a geo-distributed, heterogeneous and
#	edge-computing environment. 
#-------------------------------------------#
import random
import gurobipy as gp
from gurobipy import GRB
from random import choices
import copy
from LPOptimizer import LPOptimizer
from DAGCreator import DAGCreator
from SpringRelaxation import SpringRelaxation
from EqualAssignment import EqualAssignment
import sys
from FindMetrics import findMetricsFun

class Equality():

	def divideNumber(parts):  # Function to divide a random into n unequal parts
		sumPercentages = 0
		while (sumPercentages == 0):
			randomPercentages = []
			dividedNumberParts = []
			for i in range(parts):
				if (random.randint(0, 5) > 3):
					randomPercentages.append(random.randint(0, 10))
					sumPercentages += randomPercentages[i]
				else:
					randomPercentages.append(0)
		for i in range(parts):
			dividedNumberParts.append(round(randomPercentages[i] / sumPercentages, 2))
		return dividedNumberParts

	global numberOfDevices	

	#ARG1:number of operators-tested values: 5, 10, 15
	#ARG2:beta for F formula-tested values: 0.5, 1, 1.5, 2, 3 
	#ARG3:alpha penalty (the bigger the value the smaller the penalty)-tested values: 5, 10
	#ARG4:threshold for qualOpt-tested values: 0.75, 0.8, 0.85, 0.9, 0.95
	#ARG5:number of iterations
	#(OPTIONAL)ARG6:DQ check propagated to children nodes-tested values: 1
	numberOfOperators = int(sys.argv[1])
	beta = float(sys.argv[2])  # For F formula
	penalty = int(sys.argv[3])  # Penalty
	qThreshold = float(sys.argv[4])
	numberOfIterations = int(sys.argv[5])
	if(len(sys.argv)==7 and sys.argv[6]=="1"):
		DQMode = "Children"
	else:
		DQMode="Sources"

	f = open("FExp.txt", "a")
	f.write("n="+str(numberOfOperators)+", b="+str(beta)+", a="+str(penalty)+", threshold="+str(qThreshold)+", iterations="+str(numberOfIterations)+", DQ mode="+ DQMode+"\n")
	f.close()
	f = open("DQExp.txt", "a")
	f.write("n=" + str(numberOfOperators) + ", b=" + str(beta) + ", a=" + str(penalty) + ", threshold=" + str(
		qThreshold) + ", iterations=" + str(numberOfIterations) + ", DQ mode=" + DQMode + "\n")
	f.close()
	f = open("timeExp.txt", "a")
	f.write("n=" + str(numberOfOperators) + ", b=" + str(beta) + ", a=" + str(penalty) + ", threshold=" + str(
		qThreshold) + ", iterations=" + str(numberOfIterations) + ", DQ mode=" + DQMode + "\n")
	f.close()
	f = open("F.txt", "a")
	f.write("n=" + str(numberOfOperators) + ", b=" + str(beta) + ", a=" + str(penalty) + ", threshold=" + str(
		qThreshold) + ", iterations=" + str(numberOfIterations) + ", DQ mode=" + DQMode + "\n")
	f.close()
	f = open("DQ.txt", "a")
	f.write("n=" + str(numberOfOperators) + ", b=" + str(beta) + ", a=" + str(penalty) + ", threshold=" + str(
		qThreshold) + ", iterations=" + str(numberOfIterations) + ", DQ mode=" + DQMode + "\n")
	f.close()
	f = open("time.txt", "a")
	f.write("n=" + str(numberOfOperators) + ", b=" + str(beta) + ", a=" + str(penalty) + ", threshold=" + str(
		qThreshold) + ", iterations=" + str(numberOfIterations) + ", DQ mode=" + DQMode + "\n")
	f.close()
	f = open("FExp.txt", "a")
	f.write("LP Spring latOpt qualOpt Equal solvedBy \n")
	f.close()
	f = open("DQExp.txt", "a")
	f.write("LP Spring latOpt qualOpt Equal solvedBy \n")
	f.close()
	f = open("timeExp.txt", "a")
	f.write("LP Spring latOpt qualOpt Equal solvedBy \n")
	f.close()


	#DEFINE GRAPH
	for iter1 in range(0,8):
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
			f = open("FExp.txt", "a")
			f.write("Replicated" + "\n")
			f.close()
			f = open("DQExp.txt", "a")
			f.write("Replicated" + "\n")
			f.close()
			f = open("timeExp.txt", "a")
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
			f = open("FExp.txt", "a")
			f.write("Diamond" + "\n")
			f.close()
			f = open("DQExp.txt", "a")
			f.write("Diamond" + "\n")
			f.close()
			f = open("timeExp.txt", "a")
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
			f = open("FExp.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
			f = open("DQExp.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
			f = open("timeExp.txt", "a")
			f.write("Sequential" + "\n")
			f.close()
		else:
			 f = open("F.txt", "a")
			 f.write("Medium"+str(iter1)+ "\n")
			 f.close()
			 f = open("DQ.txt", "a")
			 f.write("Medium"+str(iter1) +"\n")
			 f.close()
			 f = open("time.txt", "a")
			 f.write("Medium"+str(iter1)+ "\n")
			 f.close()
			 f = open("FExp.txt", "a")
			 f.write("Medium" + str(iter1) + "\n")
			 f.close()
			 f = open("DQExp.txt", "a")
			 f.write("Medium" + str(iter1) + "\n")
			 f.close()
			 f = open("timeExp.txt", "a")
			 f.write("Medium" + str(iter1) + "\n")
			 f.close()
		for iter2 in range(numberOfIterations):
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
			(paths,pairs,source,sink,children,parents,numberOfOperators,noOfSources,sourceChild)=dag.run()

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
				RCpu.append(round(random.uniform(1, 10),2))
				RMem.append(random.randint(100, 1000))
			RCPUDQ=round(random.uniform(1, 10),2)
			RRAMDQ=random.randint(100, 1000)

			#Characteristics of devices
			CCpu=[]
			CMem=[]
			UsedCpu=[]
			UsedMem=[]
			for i in range(numberOfDevices):
				CCpu.append(round(random.uniform(1, 10),2))
				CMem.append(random.randint(100, 1000))
				UsedCpu.append(0)
				UsedMem.append(0)

			alpha=meancomCost/penalty#Penalty
			
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
			(DQfractionSpring,totalTransferTimeSrping,FSpring)=sr.run(numberOfDevices,comCost,numberOfOperators,paths,pairs,source,sourceChild,children,parents,RCpu,RMem,CCpu,CMem,available,fractions,RCPUDQ,RRAMDQ,noOfSources,beta,alpha,DQMode)

			#Εqual distribution
			eq=EqualAssignment()
			(DQfractionEQ,totalTransferTimeEQ,FEQ,UsedCpuEQ,UsedMemEQ,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,slowestPathEQ)=eq.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents,paths,source,sourceChild,noOfSources,fractions,beta,alpha,DQMode)
			
			#LP initial solution
			transferTimes={}
			slowestDevices={}
			for op in range(numberOfOperators):
				if(op not in source):#For each non source
					modelN = gp.Model("lp")
					modelN.Params.OutputFlag = 0
					transferTime = modelN.addVar(vtype=GRB.CONTINUOUS, lb=0, name="transferTime") #The objective
					fList = list(range(numberOfDevices))
					fraction = modelN.addVars(fList, lb=0, ub=100, vtype=GRB.INTEGER, name="x")
					binZerol = list(range(numberOfDevices))
					binSelfl = list(range(numberOfDevices))
					binZero = modelN.addVars(binZerol, lb=0, ub=1, vtype=GRB.INTEGER, name="bz") #0 if the destination device will not hold data, 1 otherwise
					binSelf = modelN.addVars(binSelfl, lb=0, ub=1, vtype=GRB.INTEGER, name="bs") #1 if the destination device is the same as the starting one, 0 otherwise

					c1 = 0
					for i in range(numberOfDevices):#CPU, RAM constraints
						modelN.addConstr((fraction[i] * RCpu[op] / 100) <= (CCpu[i] - UsedCpu[i]))
						modelN.addConstr((fraction[i] * RMem[op] / 100) <= (CMem[i] - UsedMem[i]))
						c1+=fraction[i]#Sum of fractions equal to 100
						if(available[op][i]==0):#Availability constraint
							modelN.addConstr(fraction[i] == 0)
						modelN.addConstr(binSelf[i] >= fraction[i] / 100)
						modelN.addConstr(binSelf[i] <= fraction[i] / 100 + 0.99)
						modelN.addConstr(binZero[i] >= fraction[i] / 100)
						modelN.addConstr(binZero[i] <= fraction[i] / 100 + 0.99)
					modelN.addConstr(c1 == 100)

					for op1 in parents[op]:
						for dev1 in range(numberOfDevices):
							c2 = 0
							for dev2 in range(numberOfDevices):
								c2 = c2 + (fractions[op1][dev1] * pairs[op1, op] * comCost[dev1][dev2] * fraction[dev2]) / 100
								if (fractions[op1][dev1] != 0):#If the device holds data
									c2 = c2 + alpha * (binZero.sum() - binSelf[dev1]) #Penalty for each enabled link to other device
							modelN.addConstr(transferTime >= c2)#Minimize the max
					modelN.setObjective(transferTime, GRB.MINIMIZE)

					modelN.optimize()#Solve LP
					if (modelN.STATUS != GRB.OPTIMAL):
						solved = -1
					else:
						solved = 1
					if(solved==-1):
						break;
					#Enforce solution
					for i in range(numberOfDevices):
						fractions[op].append(round(fraction[i].x)/100)
						UsedCpu[i]=UsedCpu[i]+(fraction[i].x*RCpu[op]/100)
						UsedMem[i]=UsedMem[i]+(fraction[i].x*RMem[op]/100)
			
					
			if(solved!=-1 ):#If a solution was found by LP
				#Find metrics (latency, dq fraction, F)
				(F,totalTransferTime,DQfraction,DQfractions,usedCpu,usedMem,transferTimes,slowestDevices,slowestPath)=findMetricsFun(numberOfDevices, comCost, paths, pairs, noOfSources, beta, alpha, available, RCPUDQ, RRAMDQ,
							CCpu,
							CMem, source, sourceChild, fractions, UsedCpu, UsedMem,DQMode)

				#Call optimizer and find best solution
				lpopt=LPOptimizer()
				if(FEQ<F and FEQ!=0):#If equal found a better solution than LP
					#Call optimizer using equal solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpuEQ,UsedMemEQ,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,sourceChild,noOfSources,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,DQfractionEQ,totalTransferTimeEQ,FEQ,beta,alpha,slowestPathEQ,qThreshold,DQMode)
					if(FSpring<FLat and FSpring<FQual and FSpring!=0):#If Spring Relaxation found a better solution than qualOpt and latOpt the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is solved by Equal assignment
						solvedBy="Equal"
				else:#If LP is better than equal
					#Call optimizer using LP solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpu,UsedMem,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,sourceChild,noOfSources,fractions,transferTimes,slowestDevices,DQfractions,DQfraction,totalTransferTime,F,beta,alpha,slowestPath,qThreshold,DQMode)
					if(FSpring<FLat and FSpring<FQual and FSpring!=0):#If Spring Relaxation found a better solution than qualOpt and latOpt the problem is solved by Spring Relaxation
						solvedBy="Spring"
					else:#Else the problem is solved by LP
						solvedBy="LP"

			else:#Id LP found no solution
				if(FEQ!=0):#If Equal found a solution
					#Call optimizer using equal solution
					(DQfractionLat,totalTransferTimeLat,FLat,DQfractionQual,totalTransferTimeQual,FQual)=lpopt.run(numberOfOperators,numberOfDevices,RCpu,RMem,CCpu,CMem,UsedCpuEQ,UsedMemEQ,RCPUDQ,RRAMDQ,available,comCost,pairs,parents
,paths,source,sourceChild,noOfSources,fractionsEQ,transferTimesEQ,slowestDevicesEQ,DQfractionsEQ,DQfractionEQ,totalTransferTimeEQ,FEQ,beta,alpha,slowestPathEQ,qThreshold,DQMode)
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

			f = open("FExp.txt", "a")
			f.write(str(F) + " " + str(FSpring) + " " + str(FLat) + " " + str(FQual) + " " + str(FEQ) + " " + solvedBy + "\n")
			f.close()
			f = open("DQExp.txt", "a")
			f.write(str(DQfraction) + " " + str(DQfractionSpring) + " " + str(DQfractionLat) + " " + str(DQfractionQual) + " " + str(DQfractionEQ) + " " + solvedBy + "\n")
			f.close()
			f = open("timeExp.txt", "a")
			f.write(str(totalTransferTime) + " " + str(totalTransferTimeSrping) + " " + str(totalTransferTimeLat) + " " + str(totalTransferTimeQual) + " " + str(totalTransferTimeEQ) + " " + solvedBy + "\n")
			f.close()

