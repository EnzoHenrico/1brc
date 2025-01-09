package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

type stationData struct {
	min   float32
	max   float32
	med   float32
	acc   float32
	count int
}

func main() {
	start := time.Now()
	file, err := os.Open("/home/enzohenrico/1brc/measurements.txt")
	//file, err := os.Open("test_data.txt")

	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	stationsMap := make(map[string]*stationData)
	var keysList []string
	keyListLen := 0

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading line:", err)
			return
		}
		name, temp := parseLine(line)

		station, ok := stationsMap[name]
		if !ok {
			stationsMap[name] = &stationData{temp, temp, temp, temp, 1}
			keysList = append(keysList, name)
			keyListLen++
		} else {
			if temp < station.min {
				station.min = temp
			}
			if temp > station.max {
				station.max = temp
			}
			station.count++
			station.acc += temp
		}
	}
	parseTime := time.Since(start)

	startSort := time.Now()
	sort.Strings(keysList)
	sortTime := time.Since(startSort)

	startPrinting := time.Now()
	for i := 0; i < keyListLen; i++ {
		data := stationsMap[keysList[i]]
		fmt.Println(keysList[i], data.min, data.acc/float32(data.count), data.max)
	}
	printingTime := time.Since(startPrinting)

	fmt.Println("\nParse Time : ", parseTime)
	fmt.Println("Sort Time : ", sortTime)
	fmt.Println("Printing Time : ", printingTime)
	fmt.Println("Total Time : ", time.Since(start))
}

func parseLine(line string) (string, float32) {
	var splitIndex int
	lineLength := len(line)

	for i := 0; i < lineLength; i++ {
		if line[i] == ';' {
			splitIndex = i
			break
		}
	}

	stationName := line[0:splitIndex]
	tempString := line[splitIndex+1 : lineLength-1]

	tempFloat, _ := strconv.ParseFloat(tempString, 32)

	return stationName, float32(tempFloat)
}
