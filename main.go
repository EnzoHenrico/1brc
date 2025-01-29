package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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
	mainPath = "/home/enzohenrico/1brc/measurements.txt"
	testPath = "test_data.txt"
)

func main() {
	path := mainPath

	var mapsSlice []map[string]*stationData
	var keysList []string
	var wg sync.WaitGroup

	chunks := make(chan string, 10)
	mainMap := make(map[string]*stationData)

	start := time.Now()
	for i := 0; i < 7; i++ {
		wg.Add(1)
		stationsMap := make(map[string]*stationData)
		go func(id int) {
			fmt.Println("Start Worker ID=", id+1)
			defer wg.Done()
			for chunk := range chunks {
				scanner := bufio.NewScanner(strings.NewReader(chunk))
				for scanner.Scan() {
					name, temp := ParseLine(scanner.Text())
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
			fmt.Println("Finished Parse Chunk, worker ID=", id+1)
			mapsSlice = append(mapsSlice, stationsMap)
		}(i)
	}

	go func() {
		defer close(chunks)
		file, err := os.Open(path)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}
		defer func() {
			if err := file.Close(); err != nil {
				fmt.Println("Error closing file:", err)
			}
		}()

		reader := bufio.NewReader(file)
		chunkSize := 1 << 25
		buf := make([]byte, chunkSize)
		var carriedRemaining []byte
		//var builder strings.Builder

		readStart := time.Now()
		for {
			n, err := io.ReadFull(reader, buf)
			chunk := buf[:n]
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if len(carriedRemaining) > 0 {
					chunks <- string(append(carriedRemaining, chunk...))
				} else {
					chunks <- string(chunk)
				}
				return
			} else if err != nil {
				fmt.Println("Error reading chunk:", err)
				return
			}

			if len(carriedRemaining) > 0 {
				chunk = append(carriedRemaining, chunk...)
				carriedRemaining = nil
			}
			lastLineBreakIndex := bytes.LastIndexByte(chunk, '\n')
			if lastLineBreakIndex == -1 {
				carriedRemaining = append(carriedRemaining, chunk...)
				continue
			}
			chunkData := chunk[:lastLineBreakIndex+1]
			remaining := chunk[lastLineBreakIndex+1:]

			chunks <- string(chunkData)

			if len(remaining) > 0 {
				carriedRemaining = append(carriedRemaining, remaining...)
			}

			fmt.Println("-> Sent Chunk... | Time: ", time.Since(readStart))
			readStart = time.Now()
		}
	}()
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

	fmt.Println("\nParse Time : ", parseTime)
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
