// Package parser contains structure for words and tasks.
// It unmarshals json data to these structures.
// Returns slices of words and tasks structs.
package parser

import (
	"encoding/json"
	"fmt"
	"unsafe"
)

// Task is a single task from json tasks data.
type Task struct {
	TaskData string `json:"task" db:"task"`
	Level    string `json:"level" db:"level"`
	Answer   string `json:"answer" db:"answer"`
	Position int32  `json:"position" db:"position"`
	Theme    string `json:"theme" db:"theme"`
}

// Word is a single word from json words data.
type Word struct {
	Word        string `json:"word" db:"word"`
	Explain     string `json:"explain" db:"explain"`
	Level       string `json:"level" db:"level"`
	FirstLetter string `json:"first_letter" db:"first_letter"`
	Serial      int32  `json:"serial" db:"serial"`
}

// ParseTask parses json data to tasks struct.
func ParseTask(data string, tasks *[]Task) error {
	const op = "parser.ParseTask"

	dataBytes := unsafe.Slice(unsafe.StringData(data), len(data))

	if err := json.Unmarshal(dataBytes, tasks); err != nil {
		return fmt.Errorf("%s: parse tasks: %w", op, err)
	}

	return nil
}

// ParseWords parses json data to words struct.
func ParseWords(data string, words *[]Word) error {
	const op = "parser.ParseWord"

	dataBytes := unsafe.Slice(unsafe.StringData(data), len(data))

	if err := json.Unmarshal(dataBytes, words); err != nil {
		return fmt.Errorf("%s: parse words: %w", op, err)
	}
	return nil
}
