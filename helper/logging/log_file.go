package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/logutils"
)

var (
	now = time.Now
)

// LogFile is used to setup a file based logger that also performs log rotation
type LogFile struct {
	// Log level Filter to filter out logs that do not matcch LogLevel criteria
	LogFilter *logutils.LevelFilter

	//Name of the log file
	FileName string

	//Path to the log file
	LogPath string

	//Duration between each file rotation operation
	Duration time.Duration

	//LastCreated represents the creation time of the latest log
	LastCreated time.Time

	//FileInfo is the pointer to the current file being written to
	FileInfo *os.File

	//MaxBytes is the maximum number of desired bytes for a log file
	MaxBytes int

	//BytesWritten is the number of bytes written in the current log file
	BytesWritten int64

	// Max rotated files to keep before removing them.
	MaxFiles int

	//acquire is the mutex utilized to ensure we have no concurrency issues
	acquire sync.Mutex
}

func (l *LogFile) fileNamePattern() string {
	// Extract the file extension
	fileExt := filepath.Ext(l.FileName)
	// If we have no file extension we append .log
	if fileExt == "" {
		fileExt = ".log"
	}
	// Remove the file extension from the filename
	return strings.TrimSuffix(l.FileName, fileExt) + "-%s" + fileExt
}

func (l *LogFile) openNew() error {
	newfilePath := filepath.Join(l.LogPath, l.FileName)

	// Try creating or opening the active log file. Since the active log file
	// always has the same name, append log entries to prevent overwriting
	// previous log data.
	filePointer, err := os.OpenFile(newfilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	stat, err := filePointer.Stat()
	if err != nil {
		return err
	}

	l.FileInfo = filePointer
	l.BytesWritten = stat.Size()
	l.LastCreated = l.createTime(stat)
	return nil
}

func (l *LogFile) rotate() error {
	// Get the time from the last point of contact
	timeElapsed := time.Since(l.LastCreated)
	// Rotate if we hit the byte file limit or the time limit
	if (l.BytesWritten >= int64(l.MaxBytes) && (l.MaxBytes > 0)) || timeElapsed >= l.Duration {
		l.FileInfo.Close()

		// Move current log file to a timestamped file.
		rotateTime := now()
		rotatefileName := fmt.Sprintf(l.fileNamePattern(), strconv.FormatInt(rotateTime.UnixNano(), 10))
		oldPath := l.FileInfo.Name()
		newPath := filepath.Join(l.LogPath, rotatefileName)
		if err := os.Rename(oldPath, newPath); err != nil {
			return fmt.Errorf("failed to rotate log files: %v", err)
		}

		if err := l.pruneFiles(); err != nil {
			return fmt.Errorf("failed to prune log files: %v", err)
		}
		return l.openNew()
	}
	return nil
}

func (l *LogFile) pruneFiles() error {
	if l.MaxFiles == 0 {
		return nil
	}
	pattern := l.fileNamePattern()
	//get all the files that match the log file pattern
	globExpression := filepath.Join(l.LogPath, fmt.Sprintf(pattern, "*"))
	matches, err := filepath.Glob(globExpression)
	if err != nil {
		return err
	}

	// Stort the strings as filepath.Glob does not publicly guarantee that files
	// are sorted, so here we add an extra defensive sort.
	sort.Strings(matches)

	// Prune if there are more files stored than the configured max
	stale := len(matches) - l.MaxFiles
	for i := 0; i < stale; i++ {
		if err := os.Remove(matches[i]); err != nil {
			return err
		}
	}
	return nil
}

// Write is used to implement io.Writer
func (l *LogFile) Write(b []byte) (int, error) {
	// Filter out log entries that do not match log level criteria
	if !l.LogFilter.Check(b) {
		return 0, nil
	}

	l.acquire.Lock()
	defer l.acquire.Unlock()
	//Create a new file if we have no file to write to
	if l.FileInfo == nil {
		if err := l.openNew(); err != nil {
			return 0, err
		}
	}
	// Check for the last contact and rotate if necessary
	if err := l.rotate(); err != nil {
		return 0, err
	}

	n, err := l.FileInfo.Write(b)
	l.BytesWritten += int64(n)
	return n, err
}