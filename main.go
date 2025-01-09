package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
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
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	stationsMap := make(map[string]*stationData)

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
			stationsMap[name] = &stationData{
				min:   temp,
				max:   temp,
				med:   temp,
				acc:   temp,
				count: 1,
			}
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

	for key, data := range stationsMap {
		med := data.acc / float32(data.count)
		fmt.Println(key, data.min, med, data.max)
	}
	fmt.Println(time.Since(start))
}

func parseLine(line string) (string, float32) {
	isWritingName := true
	isWritingTemp := false
	stationName := ""
	temperature := ""

	for _, b := range []rune(line) {
		if b == ';' || b == '\n' {
			isWritingName = false
			isWritingTemp = true
			continue
		}
		if isWritingName {
			stationName += string(b)
		}
		if isWritingTemp {
			temperature += string(b)
		}
	}
	tempFloat, _ := strconv.ParseFloat(temperature, 32)

	return stationName, float32(tempFloat)
}
