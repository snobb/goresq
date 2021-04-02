package signal

import (
	"os"
	"os/signal"
	"syscall"
)

// QuitChannel is channel to send quit signal
type QuitChannel <-chan struct{}

// Quit is a helper function to create quit channel from OS signalst
func Quit() QuitChannel {
	sigs := make(chan os.Signal)
	done := make(chan struct{})

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- struct{}{}
	}()

	return done
}
