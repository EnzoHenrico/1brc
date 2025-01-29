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
	path := mainPath
	totalSize := billion

	countCPUs := runtime.NumCPU()
	chunkMaxSize := int(math.Floor(float64(totalSize) / float64(countCPUs*2)))

	var mapsSlice []map[string]*stationData
	var keysList []string
	var wg sync.WaitGroup

	chunks := make(chan []string, 10)
	mainMap := make(map[string]*stationData)

	start := time.Now()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stationsMap := make(map[string]*stationData)
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
		}()
	}

	go func() {
		defer close(chunks)

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
		lines := make([]string, 0, chunkMaxSize)
		readStart := time.Now()
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Println("Error reading line:", err)
				return
			}
			lines = lines[:count+1]
			lines[count] = line
			count++
			if count == chunkMaxSize {
				fmt.Println("-> Sent Chunk... | Time: ", time.Since(readStart))
				chunks <- lines
				if countTotal < totalSize-chunkMaxSize {
					lines = make([]string, 0, chunkMaxSize)
					count = 0
				}
				readStart = time.Now()
			}
			countTotal++
		}
	}()
	readTime := time.Since(start)
	startParse := time.Now()

	wg.Wait()
	parseTime := time.Since(startParse)

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

	sort.Strings(keysList)

	for i := 0; i < len(keysList); i++ {
		data := mainMap[keysList[i]]
		fmt.Println(keysList[i], data.min, data.acc/float32(data.count), data.max)
	}

	fmt.Println("\nRead Time : ", readTime)
	fmt.Println("Parse Time : ", parseTime)
	fmt.Println("Merge Time : ", mergeTime)
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
