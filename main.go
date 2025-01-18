package main

import (
	"bufio"
	"fmt"
	"io"
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

type workerRequest struct {
	line        string
	stationsMap map[string]*stationData
	keysList    *[]string
}

const (
	path     = "/home/enzohenrico/1brc/measurements.txt"
	testPath = "test_data.txt"
)

func worker(requests <-chan workerRequest, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()
	for r := range requests {
		name, temp := ParseLine(r.line)
		mu.Lock()
		station, ok := r.stationsMap[name]
		mu.Unlock()
		if !ok {
			mu.Lock()
			r.stationsMap[name] = &stationData{temp, temp, temp, temp, 1}
			*r.keysList = append(*r.keysList, name)
			mu.Unlock()
		} else {
			mu.Lock()
			if temp < station.min {
				station.min = temp
			}
			if temp > station.max {
				station.max = temp
			}
			station.count++
			station.acc += temp
			mu.Unlock()
		}
	}
}

func main() {
	start := time.Now()
	countCPUs := runtime.NumCPU()

	var keysList []string
	var wg sync.WaitGroup
	var mu sync.Mutex

	stationsMap := make(map[string]*stationData)
	channel := make(chan workerRequest)

	for i := 0; i < countCPUs; i++ {
		wg.Add(1)
		go worker(channel, &wg, &mu)
	}

	file, err := os.Open(testPath)
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
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading line:", err)
			return
		}
		channel <- workerRequest{
			line,
			stationsMap,
			&keysList,
		}
	}

	close(channel)
	wg.Wait()

	parseTime := time.Since(start)

	startSort := time.Now()
	sort.Strings(keysList)
	sortTime := time.Since(startSort)

	startPrinting := time.Now()
	for i := 0; i < len(keysList); i++ {
		data := stationsMap[keysList[i]]
		fmt.Println(keysList[i], data.min, data.acc/float32(data.count), data.max)
	}
	printingTime := time.Since(startPrinting)

	fmt.Println("\nParse Time : ", parseTime)
	fmt.Println("Sort Time : ", sortTime)
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
