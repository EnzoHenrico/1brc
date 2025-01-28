package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

type stationData struct {
	min   float32
	max   float32
	med   float32
	acc   float32
	count int
}

const (
	mainPath    = "/home/enzohenrico/1brc/measurements.txt"
	testPath    = "test_data.txt"
	billion     = 1000000000
	twoMillions = 2000000
)

func main() {
	path := testPath
	totalSize := twoMillions

	start := time.Now()
	countCPUs := runtime.NumCPU()
	chunkMaxSize := int(math.Floor(float64(totalSize) / float64(countCPUs)))

	var mapsSlice []map[string]*stationData
	var keysList []string
	var wg sync.WaitGroup

	chunks := make(chan []string, countCPUs)
	mainMap := make(map[string]*stationData)

	for i := 0; i < countCPUs; i++ {
		wg.Add(1)
		stationsMap := make(map[string]*stationData)
		go func(workerID int, ch <-chan []string) {
			defer wg.Done()
			for chunk := range chunks {
				for i := 0; i < len(chunk); i++ {
					name, temp := ParseLine(chunk[i])
					station, ok := stationsMap[name]
					if !ok {
						stationsMap[name] = &stationData{temp, temp, temp, temp, 1}
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
			}
			mapsSlice = append(mapsSlice, stationsMap)
		}(i, chunks)
	}

	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println("Error closing file:", err)
		}
	}(file)

	reader := bufio.NewReader(file)
	count := 0
	countTotal := 0
	var lines []string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading line:", err)
			return
		}
		lines = append(lines, line)
		if count == chunkMaxSize {
			chunks <- lines
			if countTotal < totalSize-chunkMaxSize {
				lines = nil
				count = 0
			}
		}
		count++
		countTotal++
	}
	parseTime := time.Since(start)

	close(chunks)
	wg.Wait()

	startMerge := time.Now()
	for i := 0; i < len(mapsSlice); i++ {
		for name, currentStation := range mapsSlice[i] {
			stationInMain, ok := mainMap[name]
			if !ok {
				mainMap[name] = currentStation
				keysList = append(keysList, name)
			} else {
				if currentStation.min < stationInMain.min {
					stationInMain.min = currentStation.min
				}
				if currentStation.max > stationInMain.max {
					stationInMain.max = currentStation.max
				}
				stationInMain.count++
				stationInMain.acc += currentStation.acc
			}
		}
	}
	mergeTime := time.Since(startMerge)

	startSort := time.Now()
	sort.Strings(keysList)
	sortTime := time.Since(startSort)

	startPrinting := time.Now()
	for i := 0; i < len(keysList); i++ {
		data := mainMap[keysList[i]]
		fmt.Println(keysList[i], data.min, data.acc/float32(data.count), data.max)
	}
	printingTime := time.Since(startPrinting)

	fmt.Println("\nParse Time : ", parseTime)
	fmt.Println("Sort Time : ", sortTime)
	fmt.Println("Merge Time : ", mergeTime)
	fmt.Println("Printing Time : ", printingTime)
	fmt.Println("Total Time : ", time.Since(start))
}

func ParseLine(line string) (string, float32) {
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
