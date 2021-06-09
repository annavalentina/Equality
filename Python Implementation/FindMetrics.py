import copy



def findMetricsFun( numberOfDevices, comCost, paths, pairs, noOfSources, beta, alpha, available, RCPUDQ, RRAMDQ, CCpu,
                CMem, source, sourceChild,fractions,usedCpu,usedMem):

    transferTimes = {}
    slowestDevices = {}
    # Find new total time
    for j in pairs:  # Find cost of each pair
        op1 = j[0]
        op2 = j[1]
        max = -1
        sum = 0
        slowest = -1
        # Find total number of enabled links (communication between devices)
        for dev1 in range(numberOfDevices):
            enabledLinks = 0
            for dev2 in range(numberOfDevices):
                if (fractions[op1][dev1] != 0 and fractions[op2][dev2] != 0 and dev1 != dev2):
                    enabledLinks += 1
                sum = sum + (
                            fractions[op1][dev1] * pairs[op1, op2] * comCost[dev1][dev2] * fractions[op2][dev2])
            sum = sum + alpha * enabledLinks  # Penalty in case of multiple communication links enabled
            if (sum > max):
                max = sum
                slowest = dev1
            sum = 0
        transferTimes[(op1, op2)] = round(max, 3)
        slowestDevices[(op1, op2)] = slowest

    # Find slowest path
    slowestPath = -1
    counter = 0
    totalTransferTime = 0
    for path in paths:
        transTime = 0
        for i in range(len(path) - 1):
            transTime = transTime + transferTimes[path[i], path[i + 1]]
        if (transTime > totalTransferTime):
            totalTransferTime = transTime
            slowestPath = counter
        counter += 1

    # Calculate new maximum DQ fraction possible
    DQfractions = {}
    sum = 0
    for op in source:
        sourceTemp = []
        childTemp = {}
        sourceSum = 0
        childSum = 0
        currUsedCpu = copy.deepcopy(usedCpu)
        currUsedMem = copy.deepcopy(usedMem)
        currUsedCpuC = copy.deepcopy(usedCpu)
        currUsedMemC = copy.deepcopy(usedMem)
        for dev in range(numberOfDevices):
            if (available[op][dev] == 1 and fractions[op][dev] != 0):
                a = (CCpu[dev] - currUsedCpu[dev]) / (RCPUDQ * fractions[op][dev])
                b = (CMem[dev] - currUsedMem[dev]) / (RRAMDQ * fractions[op][dev])
                if (a < b):
                    x = a
                else:
                    x = b
                if (x > 1):
                    x = 1
                if (x < 0):
                    x = 0
                x = round(x, 2)
                currUsedCpu[dev] = currUsedCpu[dev] + x * RCPUDQ * fractions[op][dev]
                currUsedMem[dev] = currUsedMem[dev] + x * RRAMDQ * fractions[op][dev]
                sourceTemp.append(x)
                sourceSum += fractions[op][dev] * x
            else:
                sourceTemp.append(0)

        # Assignment of dq check to devices that hold fraction of the children nodes
        for child in sourceChild[op]:
            childTemp[child] = []
            for dev in range(numberOfDevices):
                if (available[child][dev] == 1 and fractions[child][dev] != 0):
                    a = (CCpu[dev] - currUsedCpuC[dev]) / (RCPUDQ * fractions[child][dev])
                    b = (CMem[dev] - currUsedMemC[dev]) / (RRAMDQ * fractions[child][dev])
                    if (a < b):
                        x = a
                    else:
                        x = b
                    if (x > 1):
                        x = 1
                    if (x < 0):
                        x = 0
                    x = round(x, 2)
                    currUsedCpuC[dev] = currUsedCpuC[dev] + x * fractions[child][dev] * RCPUDQ
                    currUsedMemC[dev] = currUsedMemC[dev] + x * fractions[child][dev] * RRAMDQ
                    childTemp[child].append(x)
                    childSum += fractions[child][dev] * x
                else:
                    childTemp[child].append(0)
        childSum = childSum / len(sourceChild[op])
        if (childSum > sourceSum):  # If it is beneficial to assign dq check to children nodes
            for child in sourceChild[op]:
                DQfractions[child] = copy.deepcopy(childTemp[child])
                usedCpu = copy.deepcopy(currUsedCpuC)
                usedMem = copy.deepcopy(currUsedMemC)
            sum += childSum
        else:  # Else assign dq check to source
            DQfractions[op] = copy.deepcopy(sourceTemp)
            usedCpu = copy.deepcopy(currUsedCpu)
            usedMem = copy.deepcopy(currUsedMem)
            sum += sourceSum

    for dev in range(numberOfDevices):
        usedCpu[dev] = round(usedCpu[dev], 2)
        usedMem[dev] = round(usedMem[dev], 2)
    DQfraction = (1 / noOfSources) * sum
    if (DQfraction > 1):  # Due to float operations
        DQfraction = 1
    F = totalTransferTime / (1 + beta * DQfraction)
    return (F,totalTransferTime,DQfraction,DQfractions,usedCpu,usedMem,transferTimes,slowestDevices,slowestPath)
    #return (F, totalTransferTime, slowestPath, DQfractions, DQfraction,usedCpu,usedMem)






