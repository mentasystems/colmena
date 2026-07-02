package colmena

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// NewResendAlerter returns a BackupConfig.OnError handler that emails backup
// engine failures through Resend (https://resend.com), at most once per hour
// per database — a persistent outage pages once, not once per sync tick.
// Every error is also logged. With an empty apiKey it only logs.
func NewResendAlerter(apiKey, from, to, service string) func(db string, err error) {
	var mu sync.Mutex
	lastSent := map[string]time.Time{}
	client := &http.Client{Timeout: 15 * time.Second}

	return func(db string, err error) {
		log.Printf("%s: BACKUP ERROR db=%s: %v", service, db, err)
		if apiKey == "" || to == "" {
			return
		}
		mu.Lock()
		if time.Since(lastSent[db]) < time.Hour {
			mu.Unlock()
			return
		}
		lastSent[db] = time.Now()
		mu.Unlock()

		body, _ := json.Marshal(map[string]any{ // any-ok: JSON request body
			"from":    from,
			"to":      []string{to},
			"subject": fmt.Sprintf("[%s] backup engine error (%s)", service, db),
			"text": "The continuous backup engine reported an error and will keep retrying.\n\n" +
				"service: " + service + "\ndatabase: " + db + "\nerror: " + err.Error() + "\n\n" +
				"Check: journalctl -u " + service + " | grep -i backup",
		})
		req, reqErr := http.NewRequest(http.MethodPost, "https://api.resend.com/emails", bytes.NewReader(body))
		if reqErr != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		resp, sendErr := client.Do(req)
		if sendErr != nil {
			log.Printf("%s: backup alert email failed: %v", service, sendErr)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			log.Printf("%s: backup alert email status %d", service, resp.StatusCode)
		}
	}
}
