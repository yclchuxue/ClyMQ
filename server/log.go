package server

import (
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var mu sync.Mutex

func LOGinit() {
	mu.Lock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	// log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))

	// log.SetPrefix("【ClyMQ】")
	log.SetFlags(log.LstdFlags | log.Lshortfile |log.LUTC)
	
	mu.Unlock()
}

func DEBUG(topic logTopic, format string, a ...interface{}) {
	// pc, file, lineNo, ok := runtime.Caller(1)
	_, file, lineNo, ok := runtime.Caller(1)

	if !ok {
		log.Println("runtime.Caller() failed")
	}
	// funcName := runtime.FuncForPC(pc).Name()
	fileName := path.Base(file) // Base函数返回路径的最后一个元素

	if 3 >= 1 {
		mu.Lock()
		// time := time.Since(debugStart).Microseconds()
		// time = time / 100
		// prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		prefix := fmt.Sprintf("%v ", string(topic))
		format = prefix + fileName + " line: " + strconv.Itoa(lineNo) + " " + format
		fmt.Printf(format, a...)
		mu.Unlock()
	}
}
