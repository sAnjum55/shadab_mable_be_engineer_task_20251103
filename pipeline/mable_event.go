package pipeline

import "time"

// Simplified version of the Mable Event JSON for testing
// You can include more fields if needed, but this covers main nested data.
type MableEvent struct {
	EID string `json:"eid"`
	EN  string `json:"en"`
	TS  int64  `json:"ts"`
	BD  struct {
		AID       string `json:"aid"`
		SRW       int    `json:"srw"`
		SRH       int    `json:"srh"`
		UL        string `json:"ul"`
		IsBackend bool   `json:"is_backend"`
	} `json:"bd"`
	PD struct {
		DL string  `json:"dl"`
		DT string  `json:"dt"`
		PP float64 `json:"pp"`
		PB int     `json:"pb"`
	} `json:"pd"`
	ESD struct {
		Cart struct {
			Currency string `json:"currency"`
			Total    struct {
				Price float64 `json:"price"`
			} `json:"total"`
		} `json:"cart"`
	} `json:"esd"`
	CreatedAt time.Time `json:"created_at"`
}
