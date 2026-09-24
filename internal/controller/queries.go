package controller

import (
	"fmt"
	"time"
)

func avgServiceTimeQuery(window time.Duration) string {
	return fmt.Sprintf(
		"sum(rate(load_target_job_service_seconds_sum[%s])) / sum(rate(load_target_job_service_seconds_count[%s]))",
		window.String(),
		window.String(),
	)
}

func avgArrivedJobsRate(window time.Duration) string {
	return fmt.Sprintf("sum(rate(load_target_arrivals_total[%s]))", window.String())
}
