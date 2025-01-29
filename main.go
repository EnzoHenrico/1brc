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
	mainPath          = "/home/enzohenrico/1brc/measurements.txt"
	testPath          = "test_data.txt"
	chunkSize         = 1 << 25
	channelBufferSize = 10
	workersCount      = 7
)

func main() {
	path := mainPath

	var mapsSlice []map[string]*stationData
	var keysList []string
	var wg sync.WaitGroup

	chunkChannel := make(chan string, channelBufferSize)
	mainMap := make(map[string]*stationData)
	start := time.Now()

	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		stationsMap := make(map[string]*stationData)
		go func() {
			defer wg.Done()
			for chunk := range chunkChannel {
				lines := ParseChunks(chunk)
				for i := 0; i < len(lines); i++ {
					name, temp := ParseLine(lines[i])
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
		defer close(chunkChannel)
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
		buf := make([]byte, chunkSize)
		var carriedRemaining []byte
		//var builder strings.Builder
		for {
			n, err := io.ReadFull(reader, buf)
			chunk := buf[:n]
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if len(carriedRemaining) > 0 {
					chunkChannel <- string(append(carriedRemaining, chunk...))
				} else {
					chunkChannel <- string(chunk)
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

			chunkChannel <- string(chunkData)

			if len(remaining) > 0 {
				carriedRemaining = append(carriedRemaining, remaining...)
			}
		}
	}()
	wg.Wait()

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

	sort.Strings(keysList)

	for i := 0; i < len(keysList); i++ {
		data := mainMap[keysList[i]]
		fmt.Println(keysList[i], data.min, data.acc/float32(data.count), data.max)
	}

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

func ParseChunks(chunk string) []string {
	var result []string
	pos := 0
	chunkLen := len(chunk)
	for pos < chunkLen {
		i := strings.IndexByte(chunk[pos:], '\n')
		if i < 0 {
			break
		}
		end := pos + i
		result = append(result, chunk[pos:end])
		pos = end + 1
	}
	if pos < chunkLen {
		result = append(result, chunk[pos:])
	}
	return result
}
