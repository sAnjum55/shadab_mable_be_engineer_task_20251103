// Package pipeline defines the core components of a generic data processing
// pipeline. This file contains the MableEvent struct, a Go representation of
// the JSON event schema used in benchmarking and integration tests.
//
// Author: Shadab
// Date: November 3, 2025
package pipeline

import "time"

//
// =======================
//  MABLE EVENT STRUCTURE
// =======================
//

// MableEvent represents a simplified version of a Mable analytics event.
// It is derived from the provided JSON schema in the assignment appendix.
// The struct captures key metadata, product, and device details relevant for
// testing and benchmarking the pipeline’s ability to process nested data.
//
// Each nested structure maps directly to sections of the event JSON (e.g.,
// "bd", "pd", "esd").
//
// Note: This is a trimmed version containing the main fields necessary for
// simulation. Additional properties can be added later for full fidelity.
type MableEvent struct {
	// EID is the unique event identifier.
	EID string `json:"eid"`

	// EN represents the event name, such as "Order Completed".
	EN string `json:"en"`

	// TS is a UNIX timestamp indicating when the event occurred.
	TS int64 `json:"ts"`

	// BD contains browser/device-related metadata about the event origin.
	BD struct {
		AID       string `json:"aid"`        // Anonymous ID for the browser session.
		SRW       int    `json:"srw"`        // Screen width of the user’s device.
		SRH       int    `json:"srh"`        // Screen height of the user’s device.
		UL        string `json:"ul"`         // User language preference (e.g., "en-GB").
		IsBackend bool   `json:"is_backend"` // Whether this event originated from a backend source.
	} `json:"bd"`

	// PD holds page or product-level details from where the event originated.
	PD struct {
		DL string  `json:"dl"` // Document URL (page URL where event occurred).
		DT string  `json:"dt"` // Document title.
		PP float64 `json:"pp"` // Product price or page price-related metric.
		PB int     `json:"pb"` // Product base identifier or numeric code.
	} `json:"pd"`

	// ESD contains e-commerce session data, including cart and transaction totals.
	ESD struct {
		Cart struct {
			Currency string `json:"currency"` // Currency code, e.g., "EUR".
			Total    struct {
				Price float64 `json:"price"` // Total cart price.
			} `json:"total"`
		} `json:"cart"`
	} `json:"esd"`

	// CreatedAt stores the time when this struct was created or processed by the pipeline.
	// It is dynamically populated during pipeline execution to simulate event ingestion time.
	CreatedAt time.Time `json:"created_at"`
}
