package main

import (
	"errors"
	"fmt"
	"sort"
)

const (
	ConnectFailedEvent   = "ConnectFailed"
	TimeoutExceededEvent = "TimeoutExceeded"
	AbortedEvent         = "Aborted"
	CompletedEvent       = "Completed"
	ProgressReportEvent  = "ProgressReport"
)

type Summary struct {
	Clients           int
	TotalMessages     int
	MessagesPublished int
	Errors            int
	ErrorRate         float64
	Completed         int
	InProgress        int
	ConnectFailed     int
	TimeoutExceeded   int
	Aborted           int

	// ordered results
	PublishPerformance []float64

	PublishPerformanceMedian float64

	PublishPerformanceHistogram map[float64]float64
}

func buildSummary(nClient int, nMessages int, results []Result) (Summary, error) {

	if len(results) == 0 {
		return Summary{}, errors.New("no results collected")
	}

	totalMessages := nClient * nMessages

	nMessagesPublished := 0
	nErrors := 0
	nCompleted := 0
	nInProgress := 0
	nConnectFailed := 0
	nTimeoutExceeded := 0
	nAborted := 0

	publishPerformance := make([]float64, 0)

	for _, r := range results {
		nMessagesPublished += r.MessagesPublished

		if r.Error {
			nErrors++

			switch r.Event {
			case ConnectFailedEvent:
				nConnectFailed++
			case TimeoutExceededEvent:
				nTimeoutExceeded++
			case AbortedEvent:
				nAborted++
			}
		}

		if r.Event == CompletedEvent {
			nCompleted++
		}

		if r.Event == ProgressReportEvent {
			nInProgress++
		}

		if r.Event == CompletedEvent || r.Event == ProgressReportEvent {
			publishPerformance = append(publishPerformance, float64(r.MessagesPublished)/r.PublishTime.Seconds())
		}
	}

	if len(publishPerformance) == 0 {
		return Summary{}, errors.New("no feasible results found")
	}

	sort.Float64s(publishPerformance)

	errorRate := float64(nErrors) / float64(nClient) * 100

	return Summary{
		Clients:                     nClient,
		TotalMessages:               totalMessages,
		MessagesPublished:           nMessagesPublished,
		Errors:                      nErrors,
		ErrorRate:                   errorRate,
		Completed:                   nCompleted,
		InProgress:                  nInProgress,
		ConnectFailed:               nConnectFailed,
		TimeoutExceeded:             nTimeoutExceeded,
		Aborted:                     nAborted,
		PublishPerformance:          publishPerformance,
		PublishPerformanceMedian:    median(publishPerformance),
		PublishPerformanceHistogram: buildHistogram(publishPerformance, nCompleted+nInProgress),
	}, nil
}

func printSummary(summary Summary) {

	fmt.Println()
	fmt.Printf("# Configuration\n")
	fmt.Printf("Concurrent Clients: %d\n", summary.Clients)
	fmt.Printf("Messages / Client:  %d\n", summary.TotalMessages)

	fmt.Println()
	fmt.Printf("# Results\n")

	fmt.Printf("Published Messages: %d (%.0f%%)\n", summary.MessagesPublished, (float64(summary.MessagesPublished) / float64(summary.TotalMessages) * 100))

	fmt.Printf("Completed:          %d (%.0f%%)\n", summary.Completed, (float64(summary.Completed) / float64(summary.Clients) * 100))
	fmt.Printf("Errors:             %d (%.0f%%)\n", summary.Errors, (float64(summary.Errors) / float64(summary.Clients) * 100))

	if summary.Errors > 0 {
		fmt.Printf("- ConnectFailed:      %d (%.0f%%)\n", summary.ConnectFailed, (float64(summary.ConnectFailed) / float64(summary.Errors) * 100))
		fmt.Printf("- TimeoutExceeded:    %d (%.0f%%)\n", summary.TimeoutExceeded, (float64(summary.TimeoutExceeded) / float64(summary.Errors) * 100))
		fmt.Printf("- Aborted:            %d (%.0f%%)\n", summary.InProgress, (float64(summary.InProgress) / float64(summary.Clients) * 100))
	}

	fmt.Println()
	fmt.Printf("# Publishing Throughput\n")
	fmt.Printf("Fastest: %.0f msg/sec\n", summary.PublishPerformance[len(summary.PublishPerformance)-1])
	fmt.Printf("Slowest: %.0f msg/sec\n", summary.PublishPerformance[0])
	fmt.Printf("Median: %.0f msg/sec\n", summary.PublishPerformanceMedian)
	fmt.Println()
	printHistogram(summary.PublishPerformanceHistogram)

	fmt.Println()
}

func buildHistogram(series []float64, total int) map[float64]float64 {
	slowest := series[0]
	fastest := series[len(series)-1]

	nBuckets := 10

	steps := (fastest - slowest) / float64(nBuckets)
	bucketCount := make(map[float64]int)

	for _, v := range series {
		var tmp float64

		for i := 0; i <= nBuckets; i++ {
			f0 := slowest + steps*float64(i)
			f1 := slowest + steps*float64(i+1)

			if v >= f0 && v <= f1 {
				tmp = f1
			}
		}

		bucketCount[tmp]++
	}

	keys := make([]float64, 0)
	for k := range bucketCount {
		keys = append(keys, k)
	}

	sort.Float64s(keys)
	histogram := make(map[float64]float64)

	prev := 0.0
	for _, k := range keys {
		cur := float64(bucketCount[k])/float64(total) + prev
		histogram[k] = cur
		prev = cur
	}

	return histogram
}

func printHistogram(histogram map[float64]float64) {

	type histEntry struct {
		key   float64
		value float64
	}

	var res []histEntry
	for k, v := range histogram {
		res = append(res, histEntry{key: k, value: v})
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].key < res[j].key
	})
	for _, r := range res {
		fmt.Printf("  < %.0f msg/sec  %.0f%%\n", r.key, r.value*100)
	}

}

func median(series []float64) float64 {
	return (series[(len(series)-1)/2] + series[len(series)/2]) / 2
}
