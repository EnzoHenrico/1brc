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

type StationData struct {
	min   float32
	max   float32
	med   float32
	acc   float32
	count int
}

type SafeMap struct {
	mu sync.Mutex
	mp map[string]*StationData
}

type WorkerRequest struct {
	name        string
	temp        float32
	stationsMap *SafeMap
	keysList    *[]string
}

const (
	path     = "/home/enzohenrico/1brc/measurements.txt"
	testPath = "test_data.txt"
	billion  = 1000000000
)

func worker(requests <-chan WorkerRequest, wg *sync.WaitGroup) {
	defer wg.Done()
	for r := range requests {
		station, ok := r.stationsMap.read(r.name)
		if !ok {
			r.stationsMap.write(r.name, &StationData{r.temp, r.temp, r.temp, r.temp, 1})
			*r.keysList = append(*r.keysList, r.name)
		} else {
			r.stationsMap.update(r.temp, station)
		}
	}
}

func main() {
	start := time.Now()
	countCPUs := runtime.NumCPU()
	runtime.GOMAXPROCS(countCPUs)

	var keysList []string
	var wg sync.WaitGroup

	stationsMap := SafeMap{mp: make(map[string]*StationData)}
	keyListLen := 0
	channel := make(chan WorkerRequest)

	for i := 0; i < countCPUs; i++ {
		wg.Add(1)
		go worker(channel, &wg)
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
	lineIndex := 0
	for {
		lineIndex++
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading line:", err)
			return
		}
		name, temp := parseLine(line)
		request := WorkerRequest{
			name,
			temp,
			&stationsMap,
			&keysList,
		}
		channel <- request
	}

	close(channel)
	wg.Wait()

	parseTime := time.Since(start)

	startSort := time.Now()
	sort.Strings(keysList)
	sortTime := time.Since(startSort)

	startPrinting := time.Now()
	for i := 0; i < keyListLen; i++ {
		data := stationsMap.mp[keysList[i]]
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

func (s *SafeMap) read(name string) (*StationData, bool) {
	s.mu.Lock()
	data, ok := s.mp[name]
	s.mu.Unlock()
	return data, ok
}

func (s *SafeMap) write(name string, stationData *StationData) {
	s.mu.Lock()
	s.mp[name] = stationData
	s.mu.Unlock()
}

func (s *SafeMap) update(temp float32, station *StationData) {
	s.mu.Lock()
	if temp < station.min {
		station.min = temp
	}
	if temp > station.max {
		station.max = temp
	}
	station.count++
	station.acc += temp
	s.mu.Unlock()
}
